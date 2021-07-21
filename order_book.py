from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime

from models import Base, Order
engine = create_engine('sqlite:///orders.db')
Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)
session = DBSession()

def process_order(order):
    #Your code here
    count = session.query(Order).count()
    #insert the order to table
    order_obj = Order(id=count+1,sender_pk=order['sender_pk'],receiver_pk=order['receiver_pk'], buy_currency=order['buy_currency'], sell_currency=order['sell_currency'], buy_amount=order['buy_amount'], sell_amount=order['sell_amount'] )
    session.add(order_obj)
    session.commit()

    #Check if there are any existing orders that match
    #find the order just inserted
    target_order = session.query(Order).get(count+1)

    for i in range(count+1):
        existing_order = session.query(Order).get(i)
    	if existing_order.filled == None && existing_order.buy_currency == target_order.sell_currency && existing_order.sell_currency == target_order.buy_currency && existing_order.sell_amount / existing_order.buy_amount >= target_order.buy_amount/target_order.sell_amount && existing_order.counterparty_id == None :
            #there is a match
            existing_order.filled = target_order.filled = datetime.now()
            existing_order.counterparty_id = target_order.id
            target_order.counterparty_id = existing_order.id

            #create a new order if there's partially match
            if existing_order.sell_amount > target_order.buy_amount:
                #the existing_order has remaining amount, so it is parent
                create_by = i
                new_s_pk = existing_order.sender_pk
                new_r_pk = existing_order.receiver_pk
                new_id = count+2
                new_buy_currency = existing_order.buy_currency
                new_sell_currency = existing_order.sell_currency
                new_sell_amount = existing_order.sell_amount - target_order.buy_amount






    		
    pass