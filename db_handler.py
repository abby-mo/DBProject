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


def add_item(new_item: Item = None):
    """
    new_item - An Item object containing a new item to be inserted into the DB in the item table.
        new_item and its attributes will never be None.
    """

    print("test")
    print(new_item.item_id)
    print(new_item.product_name)
    print(new_item.brand)
    print(new_item.category)
    print(new_item.manufact)
    print(new_item.current_price)
    print(new_item.start_year)
    print(new_item.num_owned)

    # query_add_item = "INSERT into Item (i_item_sk, i_item_id, i_rec_start_date, i_product_name, i_brand, i_class, i_category, i_manufact, i_current_price, i_num_owned) VALUES ("

    # #get the i_item_sk number ()
    # query_get_item_sk = "SELECT MAX(i_item_sk) + 1 FROM Item;"
    # cur.execute(query_get_item_sk)
    # next_i_item_sk = cur.fetchall()

    
    


    #raise NotImplementedError("you must implement this function")


def add_customer(new_customer: Customer = None):
    print("ADDING CUSTOMER")
    cur.execute("SELECT MAX(c_customer_sk) + 1 FROM customer")
    new_cust_sk = cur.fetchone()[0]

    cur.execute("SELECT MAX(ca_address_sk) + 1 FROM customer_address")
    new_addr_sk = cur.fetchone()[0]

    # """wh
    # new_customer - A Customer object containing a new customer to be inserted into the DB in the customer table.
    #     new_customer and its attributes will never be None.
    # """
    raise NotImplementedError("you must implement this function")


def edit_customer(original_customer_id: str = None, new_customer: Customer = None):
    """
    original_customer_id - A string containing the customer id for the customer to be edited.
    new_customer - A Customer object containing attributes to update. If an attribute is None, it should not be altered.
    """
    raise NotImplementedError("you must implement this function")


def rent_item(item_id: str = None, customer_id: str = None):
    """
    item_id - A string containing the Item ID for the item being rented.
    customer_id - A string containing the customer id of the customer renting the item.
    """
    raise NotImplementedError("you must implement this function")


def waitlist_customer(item_id: str = None, customer_id: str = None) -> int:
    """
    Returns the customer's new place in line.
    """
    raise NotImplementedError("you must implement this function")

def update_waitlist(item_id: str = None):
    """
    Removes person at position 1 and shifts everyone else down by 1.
    """
    raise NotImplementedError("you must implement this function")


def return_item(item_id: str = None, customer_id: str = None):
    """
    Moves a rental from rental to rental_history with return_date = today.
    """
    raise NotImplementedError("you must implement this function")


def grant_extension(item_id: str = None, customer_id: str = None):
    """
    Adds 14 days to the due_date.
    """
    raise NotImplementedError("you must implement this function")


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
    query = "Select i_item_id, i_product_name, i_brand, i_category, i_manufact, i_current_price, YEAR(i_rec_start_date), i_num_owned FROM item"

    query_bits = []
    param_bits = []

    # for reference (not actually used)
    # neg_one_attributes_arr = [ (filter_attributes.current_price, "i_current_price"), (filter_attributes.start_year, "i_start_year"), (filter_attributes.num_owned, "i_num_owned")]

    # attributes
    if filter_attributes != None: 
         # creating tuples of information
        none_attributes_arr = [
        (filter_attributes.item_id, "i_item_id"),
        (filter_attributes.product_name, "i_product_name"),
        (filter_attributes.brand, "i_brand"),
        (filter_attributes.category, "i_category"),
        (filter_attributes.manufact, "i_manufact")]

        for value, column in none_attributes_arr:
            if(value != None):
                if(use_patterns == True):
                    query_bits.append(f"{column} LIKE ?")
                    param_bits.append(value)
                else:
                    query_bits.append(f"{column} = ?")
                    param_bits.append(value)


    # price
    if(min_price != -1):
        query_bits.append(f"i_current_price >= ?")
        param_bits.append(min_price)
    
    if(max_price != -1):
        query_bits.append(f"i_current_price <= ?")
        param_bits.append(max_price)
    
    # year
    if(min_start_year != -1):
        query_bits.append(f"YEAR(i_rec_start_date) >= ?")
        param_bits.append(min_start_year)
    
    if(max_start_year != -1):
        query_bits.append(f"YEAR(i_rec_start_date) <= ?")
        param_bits.append(max_start_year)
    
   
    if query_bits:
        # the list is empty
        query += " WHERE "
        for i in range(len(query_bits)):
            query += query_bits[i]
            if( i < len(query_bits) - 1):
                query += " AND "


    query += ";"

    #testing that the query works (remove later)
    print("Test print: " + query)


    # executing and fetching the query
    cur.execute(query, param_bits)
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

    if filter_attributes.customer_id is not None:
        query += f" AND c.c_customer_id {op} ?"
        params.append(filter_attributes.customer_id)
    if filter_attributes.name is not None:
        query += f" AND CONCAT(c.c_first_name, ' ', c.c_last_name) {op} ?"
        params.append(filter_attributes.name)
    if filter_attributes.email is not None:
        query += f" AND c.c_email_address {op} ?"
        params.append(filter_attributes.email)
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
    op = "LIKE" if use_patterns else "="

    if filter_attributes.item_id is not None:
        query += f" AND item_id = ?"
        params.append(filter_attributes.item_id)
    if filter_attributes.customer_id is not None:
        query += f" AND customer_id = ?"
        params.append(filter_attributes.customer_id)
    if filter_attributes.rental_date is not None:
        query += f" AND rental_date = ?"
        params.append(filter_attributes.rental_date)
    if filter_attributes.due_date is not None:
        query += f" AND due_date = ?"
        params.append(filter_attributes.due_date)
    if min_rental_date is not None:
        query += " AND rental_date >= ?"
        params.append(min_rental_date)
    if max_rental_date is not None:
        query += " AND rental_date <= ?"
        params.append(max_rental_date)
    if min_due_date is not None:
        query += " AND due_date >= ?"
        params.append(min_due_date)
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

    if filter_attributes.item_id is not None:
        query += " AND item_id = ?"
        params.append(filter_attributes.item_id)
    if filter_attributes.customer_id is not None:
        query += " AND customer_id = ?"
        params.append(filter_attributes.customer_id)
    if filter_attributes.rental_date is not None:
        query += " AND rental_date = ?"
        params.append(filter_attributes.rental_date)
    if filter_attributes.due_date is not None:
        query += " AND due_date = ?"
        params.append(filter_attributes.due_date)
    if filter_attributes.return_date is not None:
        query += " AND return_date = ?"
        params.append(filter_attributes.return_date)
    if min_rental_date is not None:
        query += " AND rental_date >= ?"
        params.append(min_rental_date)
    if max_rental_date is not None:
        query += " AND rental_date <= ?"
        params.append(max_rental_date)
    if min_due_date is not None:
        query += " AND due_date >= ?"
        params.append(min_due_date)
    if max_due_date is not None:
        query += " AND due_date <= ?"
        params.append(max_due_date)
    if min_return_date is not None:
        query += " AND return_date >= ?"
        params.append(min_return_date)
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


def get_filtered_waitlist(filter_attributes: Waitlist = None,
                          min_place_in_line: int = -1,
                          max_place_in_line: int = -1) -> list[Waitlist]:
    query = """
        SELECT item_id, customer_id, place_in_line
        FROM waitlist
        WHERE 1=1
    """
    params = []

    if filter_attributes.item_id is not None:
        query += " AND item_id = ?"
        params.append(filter_attributes.item_id)
    if filter_attributes.customer_id is not None:
        query += " AND customer_id = ?"
        params.append(filter_attributes.customer_id)
    if filter_attributes.place_in_line != -1:
        query += " AND place_in_line = ?"
        params.append(filter_attributes.place_in_line)
    if min_place_in_line != -1:
        query += " AND place_in_line >= ?"
        params.append(min_place_in_line)
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


def number_in_stock(item_id: str = None) -> int:
    """
    Returns num_owned - active rentals. Returns -1 if item doesn't exist.
    """
    
    query = """
        SELECT i_num_owned
        FROM item
        WHERE i_item_id = ?
    """
    cur.execute(query, (item_id,))
    row = cur.fetchone()

    if row is None:
        return -1

    num_owned = row[0]

    query = """
        SELECT COUNT(*)
        FROM rental
        WHERE item_id = ?
    """
    cur.execute(query, (item_id,))
    active_rentals = cur.fetchone()[0]

    return num_owned - active_rentals


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


def save_changes():
    """
    Commits all changes made to the db.
    """
    conn.commit()



def close_connection():
    """
    Closes the cursor and connection.
    """
    if cur:
        cur.close()
    if conn:
        conn.close()


