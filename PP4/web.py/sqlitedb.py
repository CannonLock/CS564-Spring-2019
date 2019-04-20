import web

db = web.database(dbn='sqlite',
        db='AuctionBase'
    )

######################BEGIN HELPER METHODS######################

# Enforce foreign key constraints
# WARNING: DO NOT REMOVE THIS!
def enforceForeignKey():
    db.query('PRAGMA foreign_keys = ON')

# initiates a transaction on the database
def transaction():
    return db.transaction()
# Sample usage (in auctionbase.py):
#
# t = sqlitedb.transaction()
# try:
#     sqlitedb.query('[FIRST QUERY STATEMENT]')
#     sqlitedb.query('[SECOND QUERY STATEMENT]')
# except Exception as e:
#     t.rollback()
#     print str(e)
# else:
#     t.commit()
#
# check out http://webpy.org/cookbook/transactions for examples

# returns the current time from your database
def getTime():
    # the correct column and table name in your database
    query_string = 'select Time from CurrentTime'
    results = query(query_string)
    return results[0].Time 

# returns a single item specified by the Item's ID in the database
# Note: if the `result' list is empty (i.e. there are no items for a
# a given ID), this will throw an Exception!
def getItemById(item_id):
    # TODO: rewrite this method to catch the Exception in case `result' is empty
    query_string = 'select * from Items where item_ID = $itemID'
    result = query(query_string, {'itemID': item_id})
    try:
        return result[0]
    except Exception as e:
        return []

# wrapper method around web.py's db.query method
# check out http://webpy.org/cookbook/query for more info
def query(query_string, vars = {}):
    return list(db.query(query_string, vars))

#####################END HELPER METHODS#####################

#TODO: additional methods to interact with your database,
# e.g. to update the current time

def searchItem(item_id, category, description, user_id, min_price, max_price, status):
    #TODO: status in {open, close, notStarted, all}

    from_table = 'Items '
    where_cond = ' '

    if category != '':
        from_table += ', Categories '
        where_cond += 'Categories.category = $category and Categories.ItemID = Items.ItemID and '
        
    if item_id != '':
        where_cond += 'Items.ItemID = $item_id and '
    
    if description != '':
        description = '%' + description + '%' 
        where_cond += 'Items.Description like $description and '
    
    if user_id != '':
        where_cond += 'Items.Seller_UserID = $user_id and '
    
    if min_price != '':
        where_cond += 'Items.Currently >= $min_price and '
    
    if max_price != '':
        where_cond += 'Items.Currently <= $max_price and '
    
    if status == 'open':
        where_cond += 'Items.Started <= $time and Items.Ends >= $time and '
    elif status == 'close':
        where_cond += 'Items.Ends <= $time and '
    elif status == 'notStarted':
        where_cond += 'Items.Started >= $time and '

    query_string = 'select * from ' + from_table + 'where' + where_cond + '1 == 1 LIMIT 10;'

    vars = {
        'item_id': item_id, 
        'user_id' : user_id,
        'max_price': max_price,
        'min_price': min_price,
        'category': category,
        'description': description,
        'time': getTime()
    }
    result = query(query_string, vars)
    return result

def addBid(user_id, product_id, price):
    query_string = ''
    result = query(query_string, {'itemID': item_id})
    return

def setTime(time): 
    query_string = 'update CurrentTime set Time = $currTime;'
    # query_string = 'select * from CurrentTime'
    db.query(query_string, {'currTime': time})
    return