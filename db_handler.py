from MARIADB_CREDS import DB_CONFIG
from mariadb import connect
from models.RentalHistory import RentalHistory
from models.Waitlist import Waitlist
from models.Item import Item
from models.Rental import Rental
from models.Customer import Customer
from datetime import date, timedelta


conn = connect(user=DB_CONFIG["username"], password=DB_CONFIG["password"], host=DB_CONFIG["host"],
               database=DB_CONFIG["database"], port=DB_CONFIG["port"])


cur = conn.cursor()

# This function was written by lilly
def add_item(new_item: Item = None):
    """
    new_item - An Item object containing a new item to be inserted into the DB in the item table.
        new_item and its attributes will never be None.
    """
   
    # inserts directly into item
    query_add_item = """
    
        INSERT into Item (i_item_sk, i_item_id, i_rec_start_date, i_product_name, i_brand, i_class, i_category, i_manufact, i_current_price, i_num_owned) 
    
        VALUES(?,?,?,?,?,?,?,?,?,?)
    """

    # #get the i_item_sk number
    query_get_item_sk = "SELECT MAX(i_item_sk) + 1 FROM Item"
    cur.execute(query_get_item_sk)
    next_i_item_sk = cur.fetchone()[0] # gets first value from tuple

    new_start_date = str(new_item.start_year) + "-01-01" # turns the year into date format

    cur.execute(query_add_item,
                 (next_i_item_sk,new_item.item_id, new_start_date,
                  new_item.product_name, new_item.brand, None,
                  new_item.category, new_item.manufact,
                  new_item.current_price, new_item.num_owned))


    #leaving class as null/none according to discussion https://ufl.instructure.com/courses/554442/discussion_topics/5080572?entry_id=29991105


    #raise NotImplementedError("you must implement this function")

def convert_address(unsplit_address: str = None):

    # The address field should be a single human-readable string, e.g.: "123 Main St, Springfield, IL 62701"

    #split full address into:
    # - 123 Main St
    # - Springfield
    # - IL 62701

    if not unsplit_address: # prevents converting errors
     return None
    
    # MUST BE NEW ADDRESS NOT FROM QUERY
    # Split full address
    address_parts = unsplit_address.split(',') 

    # split 123 Main st into street num and street name
    a_zero = address_parts[0].split(' ')
    a_street_num = a_zero[0].strip()
    a_street_name = ""

    # Loops through to add all parts of a multiword street name like Main Ave St
    for i in range(1, len(a_zero)):
        if i > 1:
            a_street_name += " "
        a_street_name += a_zero[i]

    # get city
    a_city = address_parts[1].strip()

    # split IL 62701 into state and zip code
    a_two = address_parts[2].strip().split()
    a_state = a_two[0]
    a_zip = a_two[1]

    # Packs a list for result
    addr_result = [a_street_num,a_street_name,a_city, a_state,a_zip]

    return addr_result



# This function was written by Lilly & Abby
def add_customer(new_customer: Customer = None):
    
    """
        new_customer - A Customer object containing a new customer to be inserted into the DB in the customer table.
        new_customer and its attributes will never be None.
    """

    # getting new customer address : "123 Main St, Springfield, IL 62701"

    addr_query = """
        INSERT into customer_address(ca_address_sk, ca_street_number, ca_street_name, ca_city, ca_state, ca_zip)
        VALUES(?,?,?,?,?,?)

    """

    # retrieving the next customer address sk
    get_addr_sk = """
        SELECT MAX(ca_address_sk) + 1 FROM customer_address
    """
    cur.execute(get_addr_sk)
    new_cust_addr_sk = cur.fetchone()[0] 

    #convert string address to parts
    addr_result = convert_address(new_customer.address)

    #unpack the result
    a_street_num, a_street_name, a_city, a_state, a_zip = addr_result

    cur.execute(addr_query,(new_cust_addr_sk, a_street_num,a_street_name,a_city,a_state,a_zip))


    customer_query = """
        INSERT into customer(c_customer_sk, c_customer_id, c_first_name, c_last_name, c_email_address, c_current_addr_sk) 
        VALUES(?,?,?,?,?,?)
    """

    # RETRIEVING PARAMATER DATA
    
    # retrieving the next customer_sk
    get_addr_sk = """
        SELECT MAX(c_customer_sk) + 1 FROM customer
    """
    cur.execute(get_addr_sk)
    new_cust_sk = cur.fetchone()[0]  # getting the customer's sk from result

    new_cust_id = new_customer.customer_id 

    new_cust_first_name, new_cust_last_name = new_customer.name.split(' ', 1)

    new_cust_email = new_customer.email

    # new_cust_addr_sk = was retrieved earlier 

    cur.execute(customer_query, (new_cust_sk,new_cust_id, new_cust_first_name,new_cust_last_name,new_cust_email,new_cust_addr_sk))

    # raise NotImplementedError("you must implement this function")

def edit_customer(original_customer_id: str = None, new_customer: Customer = None):
    """
    original_customer_id - A string containing the customer id for the customer to be edited.
    new_customer - A Customer object containing attributes to update. If an attribute is None, it should not be altered.
    """

    query = """
        UPDATE customer SET
    """
    param = []

    # Adds to query if customer id is changed
    if new_customer.customer_id is not None:
        query+= "c_customer_id = ?"
        param.append(new_customer.customer_id)
        # Adds comma for next case
        if new_customer.name is not None or new_customer.address is not None or new_customer.email is not None:
            query+= ","

    # Adds to query if customer name is changed
    if new_customer.name is not None:
        # building paramters
        query+= "c_first_name = ?"
        query+= ","
        query+= "c_last_name = ?"
        # splits the parsed in name 
        name_parts = new_customer.name.split(' ',1)
        param.append(name_parts[0])
        param.append(name_parts[1])
        # Adds comma for next case
        if new_customer.email is not None:
            query+= ","

    # Adds to query if customer address is changed
    if new_customer.address is not None:
        # Reterieve the current address sk from customer
        sk_address_query = """
            SELECT c_current_addr_sk FROM customer
            WHERE c_customer_id = ?
        """
        cur.execute(sk_address_query, (original_customer_id,))
        received_address_sk = cur.fetchone()[0]

        # Update customer address
        address_query = """
            UPDATE customer_address
            SET ca_street_number = ?,
            ca_street_name = ?,
            ca_city = ?,
            ca_state = ?,
            ca_zip = ?
            WHERE ca_address_sk = ?
        """

        #convert string address to parts
        addr_result = convert_address(new_customer.address)

        #unpack the result
        a_street_num, a_street_name, a_city, a_state, a_zip = addr_result

        cur.execute(address_query,
                    (a_street_num,a_street_name,a_city,a_state,a_zip,
                     received_address_sk))


    # Adds to query if customer email is changed
    if new_customer.email is not None:
        query+= "c_email_address = ?"
        param.append(new_customer.email)

    query += " WHERE c_customer_id = ?"

    param.append(original_customer_id)

    # ONLY EXECUTES if email, name or id is updated
    # WHY: because address could be updated but we wouldn't need to run this command
    if new_customer.email is not None or new_customer.name is not None or new_customer.customer_id is not None:
        cur.execute(query,param)


    #raise NotImplementedError("you must implement this function")

# This function was written by Lilly
def rent_item(item_id: str = None, customer_id: str = None):
    """
    item_id - A string containing the Item ID for the item being rented.
    customer_id - A string containing the customer id of the customer renting the item.
    """
    # Using CURDATE because of discussion post.  
    query = """
        INSERT INTO rental(item_id, customer_id, rental_date, due_date)
        VALUES(?,?,CURDATE(),DATE_ADD(CURDATE(),INTERVAL 14 DAY))
    """

    cur.execute(query,(item_id,customer_id))
    return     # return nothing

    #raise NotImplementedError("you must implement this function")

# This function was written by Lilly
def waitlist_customer(item_id: str = None, customer_id: str = None) -> int:
    """
    Returns the customer's new place in line.
    """
    insert_query = """
        Insert into waitlist(item_id,customer_id,place_in_line)
        VALUES (?,?,?)
    """

    line_length_return = line_length(item_id) # gets the length of the line using existing function

    cur.execute(insert_query, (item_id,customer_id, line_length_return+1))

    return line_length_return+1
    #raise NotImplementedError("you must implement this function")

# This function was written by Lilly
def update_waitlist(item_id: str = None):
    """
    Removes person at position 1 and shifts everyone else down by 1.
    """

    # Deletes the person at position 1
    query = """
        DELETE from waitlist
        WHERE place_in_line = 1 AND item_id = ?
    """

    cur.execute(query, (item_id,))

    # Updates all other rows
    update_query = """
        UPDATE waitlist
        SET place_in_line = place_in_line - 1 
        WHERE item_id = ?
    """
    cur.execute(update_query, (item_id,))


#    raise NotImplementedError("you must implement this function")

# This function was written by Lilly
def return_item(item_id: str = None, customer_id: str = None):
    """
    Moves a rental from rental to rental_history with return_date = today.
    """

    # insert into rental history
    insert_query = """
        INSERT INTO rental_history(item_id, customer_id, rental_date, due_date, return_date)
        VALUES(
        ?,
        
        ?,
        (SELECT rental_date FROM rental WHERE item_id = ? AND customer_id = ?),
        (SELECT due_date FROM rental WHERE item_id = ? AND customer_id = ?),
        CURDATE()
        )"""


    cur.execute(insert_query, (item_id,customer_id,item_id,customer_id,item_id,customer_id))
    #1 - values 1 - item id
    #2 - values 2 - customer id
    #3 - values 3 - select 1 - rental date
    #4 - values 3 - select 2 - rental date
    #5 - values 4 - select 1 - due date
    #6 - values 4 - select 2 - due date
    #7 - value 5 - today's date

    # delete from rental
    delete_query = """
        Delete FROM rental
        WHERE item_id = ?
        AND customer_id = ?
    """
    cur.execute(delete_query,(item_id,customer_id))

    return

    #raise NotImplementedError("you must implement this function")

# This function was written by Lilly
def grant_extension(item_id: str = None, customer_id: str = None):
    """
    Adds 14 days to the due_date.
    """
    # gets the dates
    select_date_query = """
        SELECT due_date FROM rental
        WHERE item_id = ? and customer_id = ?
    """
    cur.execute (select_date_query,(item_id,customer_id))
    rows = cur.fetchone()

    new_due_date = str(rows[0] + timedelta(days=14))

    # updates the rental
    query = """
        UPDATE rental
        SET due_date  = DATE_ADD(due_date,INTERVAL 14 DAY)
        WHERE item_id = ? AND customer_id = ?    
    """
    cur.execute(query, (item_id,customer_id))

    return

    #raise NotImplementedError("you must implement this function")


# This function was written by Lilly
def get_filtered_items(filter_attributes: Item = None,
                       use_patterns: bool = False,
                       min_price: float = -1,
                       max_price: float = -1,
                       min_start_year: int = -1,
                       max_start_year: int = -1) -> list[Item]:
    """
    Returns a list of Item objects matching the filters.
    """
    #selecting them in a specified order of the item OBJECT to make this easier to organize
    # skipping some attributes that are not needed : item sk, class
    # query = "Select i_item_id, i_product_name, i_brand, i_category, i_manufact, i_current_price, YEAR(i_rec_start_date), i_num_owned FROM itemi1"
    query = """
        SELECT i_item_id, i_product_name, i_brand, i_category, i_manufact,
               i_current_price, YEAR(i_rec_start_date), i_num_owned
        FROM item
        WHERE 1=1
    """

    query_bits = [] # holds parts of the query
    stuff_bits = [] # holds the paramters

    # for reference (not actually used)
    # neg_one_attributes_arr = [ (filter_attributes.current_price, "i_current_price"), (filter_attributes.start_year, "i_start_year"), (filter_attributes.num_owned, "i_num_owned")]

    # attributes
    if filter_attributes is not None: 
         # creating tuples of information
        none_attributes_arr = [
        (filter_attributes.item_id, "i_item_id"),
        (filter_attributes.product_name, "i_product_name"),
        (filter_attributes.brand, "i_brand"),
        (filter_attributes.category, "i_category"),
        (filter_attributes.manufact, "i_manufact")]

        for value, column in none_attributes_arr:
            if(value is not None):
                if(use_patterns == True):
                    query_bits.append(f"{column} LIKE ?")
                    stuff_bits.append(value)
                else:
                    query_bits.append(f"{column} = ?")
                    stuff_bits.append(value)

    # PRICE
    if min_price != -1:
        query_bits.append("i_current_price >= ?")
        stuff_bits.append(min_price)

    if max_price != -1:
        query_bits.append("i_current_price <= ?")
        stuff_bits.append(max_price)

    # YEAR
    if min_start_year != -1:
        query_bits.append("YEAR(i_rec_start_date) >= ?")
        stuff_bits.append(min_start_year)

    if max_start_year != -1:
        query_bits.append("YEAR(i_rec_start_date) <= ?")
        stuff_bits.append(max_start_year)
        
    if query_bits:
        query += " AND "
        for i in range(len(query_bits)):
            query += query_bits[i]
            if i < len(query_bits) - 1:
                query += " AND "

    query += " ORDER BY i_rec_start_date DESC" # making it consistently grabs the right one

    if filter_attributes is not None and filter_attributes.item_id is not None and not use_patterns:
        query += " LIMIT 1"

    # executing and fetching the query
    cur.execute(query, stuff_bits)
    rows = cur.fetchall()

    return_items = [] # intializing list of item objects

    # creating an Item object instance
    for row in rows:
        return_item = Item(
            item_id = row[0].strip(),
            product_name = row[1].strip(),
            brand = row[2].strip(), 
            category = row[3].strip(), 
            manufact = row[4].strip(),
            current_price = row[5],
            start_year = row[6],
            num_owned = row[7]
        )
        return_items.append(return_item)


    return return_items # returns a list of item objects


    #raise NotImplementedError("you must implement this function")

# this function was written by Abby
def get_filtered_customers(filter_attributes: Customer = None, use_patterns: bool = False) -> list[Customer]:
    """
    Returns a list of Customer objects matching the filters.

    """
    
    query = """
        SELECT c.c_customer_id, c.c_first_name, c.c_last_name, c.c_email_address,
               ca.ca_street_number, ca.ca_street_name, ca.ca_city, ca.ca_state, ca.ca_zip
        FROM customer c
        JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
        WHERE 1=1
    """
    params = []
    op = "LIKE" if use_patterns else "="

    # filter by customer_id
    if filter_attributes.customer_id is not None:
        query += f" AND c.c_customer_id {op} ?"
        params.append(filter_attributes.customer_id)
    # filter by name
    if filter_attributes.name is not None:
        query += f" AND CONCAT(c.c_first_name, ' ', c.c_last_name) {op} ?"
        params.append(filter_attributes.name)
    # filter by email
    if filter_attributes.email is not None:
        query += f" AND c.c_email_address {op} ?"
        params.append(filter_attributes.email)
    # filter by address
    if filter_attributes.address is not None:
        query += f" AND CONCAT(ca.ca_street_number, ' ', ca.ca_street_name, ', ', ca.ca_city, ', ', ca.ca_state, ' ', ca.ca_zip) {op} ?"
        params.append(filter_attributes.address)

    cur.execute(query, params)
    rows = cur.fetchall()

    customers = []
    for row in rows:
        address = f"{row[4].strip()} {row[5].strip()}, {row[6].strip()}, {row[7].strip()} {row[8].strip()}"
        customers.append(Customer(
            customer_id = row[0].strip(),
            name = f"{row[1].strip()} {row[2].strip()}",
            email = row[3].strip(),
            address = address
        ))
    return customers

#row[0] = customer_id
#row[1] = first_name
#row[2] = last_name
#row[3] = email
#row[4] = street number
#row[5] = street name
#row[6] = city
#row[7] = state
#row[8] = zipcode





# This was written by Abby
def get_filtered_rentals(filter_attributes: Rental = None,
                         min_rental_date: str = None,
                         max_rental_date: str = None,
                         min_due_date: str = None,
                         max_due_date: str = None) -> list[Rental]:
    """
    Returns a list of Rental objects matching the filters.
    """
    
    query = """
        SELECT item_id, customer_id, rental_date, due_date
        FROM rental
        WHERE 1=1
    """
    params = []

    # filter by item_id
    if filter_attributes.item_id is not None:
        query += f" AND item_id = ?"
        params.append(filter_attributes.item_id)
    # filter by customer_id
    if filter_attributes.customer_id is not None:
        query += f" AND customer_id = ?"
        params.append(filter_attributes.customer_id)
    # filter by rental_date
    if filter_attributes.rental_date is not None:
        query += f" AND rental_date = ?"
        params.append(filter_attributes.rental_date)
    # filter by due_date (exact match)
    if filter_attributes.due_date is not None:
        query += f" AND due_date = ?"
        params.append(filter_attributes.due_date)
    # filter by rentals on or after min_rental_date
    if min_rental_date is not None:
        query += " AND rental_date >= ?"
        params.append(min_rental_date)
    # filter by rentals on or before max_rental_date
    if max_rental_date is not None:
        query += " AND rental_date <= ?"
        params.append(max_rental_date)
    # filter by rentals on or after min_due_date
    if min_due_date is not None:
        query += " AND due_date >= ?"
        params.append(min_due_date)
    # filter by rentals on or before max_due_date
    if max_due_date is not None:
        query += " AND due_date <= ?"
        params.append(max_due_date)

    cur.execute(query, params)
    rows = cur.fetchall()

    rentals = []
    for row in rows:
        rentals.append(Rental(
            item_id=row[0].strip(),
            customer_id=row[1].strip(),
            rental_date=str(row[2]),
            due_date=str(row[3])
        ))
    return rentals

# This was written by Abby
def get_filtered_rental_histories(filter_attributes: RentalHistory = None,
                                  min_rental_date: str = None,
                                  max_rental_date: str = None,
                                  min_due_date: str = None,
                                  max_due_date: str = None,
                                  min_return_date: str = None,
                                  max_return_date: str = None) -> list[RentalHistory]:
    """
    Returns a list of RentalHistory objects matching the filters.
    """

    query = """
        SELECT item_id, customer_id, rental_date, due_date, return_date
        FROM rental_history
        WHERE 1=1
    """
    params = []

    # filter by item_id
    if filter_attributes.item_id is not None:
        query += " AND item_id = ?"
        params.append(filter_attributes.item_id)
    # filter by customer_id
    if filter_attributes.customer_id is not None:
        query += " AND customer_id = ?"
        params.append(filter_attributes.customer_id)
    # filter by rental_date
    if filter_attributes.rental_date is not None:
        query += " AND rental_date = ?"
        params.append(filter_attributes.rental_date)
    # filter by due_date
    if filter_attributes.due_date is not None:
        query += " AND due_date = ?"
        params.append(filter_attributes.due_date)
    # filter by return_date
    if filter_attributes.return_date is not None:
        query += " AND return_date = ?"
        params.append(filter_attributes.return_date)
    # filter by minimum rental date
    if min_rental_date is not None:
        query += " AND rental_date >= ?"
        params.append(min_rental_date)
    # filter by maximum rental date
    if max_rental_date is not None:
        query += " AND rental_date <= ?"
        params.append(max_rental_date)
    # filter by minimmum due date
    if min_due_date is not None:
        query += " AND due_date >= ?"
        params.append(min_due_date)
    # filter by maximum due date
    if max_due_date is not None:
        query += " AND due_date <= ?"
        params.append(max_due_date)
    # filter by minimum return date
    if min_return_date is not None:
        query += " AND return_date >= ?"
        params.append(min_return_date)
    # filter by maximum return date
    if max_return_date is not None:
        query += " AND return_date <= ?"
        params.append(max_return_date)

    cur.execute(query, params)
    rows = cur.fetchall()

    histories = []
    for row in rows:
        histories.append(RentalHistory(
            item_id=row[0].strip(),
            customer_id=row[1].strip(),
            rental_date=str(row[2]),
            due_date=str(row[3]),
            return_date=str(row[4])
        ))
    return histories

# This was written by Abby
def get_filtered_waitlist(filter_attributes: Waitlist = None,
                          min_place_in_line: int = -1,
                          max_place_in_line: int = -1) -> list[Waitlist]:
    query = """
        SELECT item_id, customer_id, place_in_line
        FROM waitlist
        WHERE 1=1
    """
    params = []

    # filter by item_id
    if filter_attributes.item_id is not None:
        query += " AND item_id = ?"
        params.append(filter_attributes.item_id)
    # filter by customer_id
    if filter_attributes.customer_id is not None:
        query += " AND customer_id = ?"
        params.append(filter_attributes.customer_id)
    # filter by place_in_line exact match
    if filter_attributes.place_in_line != -1:
        query += " AND place_in_line = ?"
        params.append(filter_attributes.place_in_line)
    # filter by place_in_line is >=
    if min_place_in_line != -1:
        query += " AND place_in_line >= ?"
        params.append(min_place_in_line)
    # filter by place_in_line is <=
    if max_place_in_line != -1:
        query += " AND place_in_line <= ?"
        params.append(max_place_in_line)

    cur.execute(query, params)
    rows = cur.fetchall()

    waitlist = []
    for row in rows:
        waitlist.append(Waitlist(
            item_id=row[0].strip(),
            customer_id=row[1].strip(),
            place_in_line=int(row[2])
        ))
    return waitlist

# This was written by Abby
def number_in_stock(item_id: str = None) -> int:
    """
    Returns num_owned - active rentals. Returns -1 if item doesn't exist.
    """
    # Selects the amount of items that exist for a certain item id
    query = """
        SELECT i_num_owned
        FROM item
        WHERE i_item_id = ?
        ORDER BY i_rec_start_date DESC
        LIMIT 1
    """
    cur.execute(query, (item_id,))
    row = cur.fetchone()

    if row is None:
        return -1

    num_owned = row[0]

    #  Selects the amount of items that are out on rental
    query = """
        SELECT COUNT(*)
        FROM rental
        WHERE item_id = ?
    """
    cur.execute(query, (item_id,))
    active_rentals = cur.fetchone()[0]

    # By subtracting the total number of the item by the amount currently being rented, we get the number of those items that is in stock
    return num_owned - active_rentals

# This was written by Abby
def place_in_line(item_id: str = None, customer_id: str = None) -> int:
    """
    Returns the customer's place_in_line, or -1 if not on waitlist.
    """
    query = """
        SELECT place_in_line
        FROM waitlist
        WHERE item_id = ?
        AND customer_id = ?
    """
    cur.execute(query, (item_id, customer_id))
    row = cur.fetchone()

    if row is None:
        return -1

    return int(row[0])

# This was written by Abby
def line_length(item_id: str = None) -> int:
    """
    Returns how many people are on the waitlist for this item.
    """
    
    query = """
        SELECT COUNT(*)
        FROM waitlist
        WHERE item_id = ?
    """
    cur.execute(query, (item_id,))
    return cur.fetchone()[0]

# This was written by team
def save_changes():
    """
    Commits all changes made to the db.
    """
    conn.commit()


# This was written by team
def close_connection():
    """
    Closes the cursor and connection.
    """
    if cur:
        cur.close()
    if conn:
        conn.close()


