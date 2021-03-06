from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime

from models import Base, Order
engine = create_engine('sqlite:///orders.db')
Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)
session = DBSession()

def get_all_match_orders(order):
    
    cur_rate=order.buy_amount / order.sell_amount
    select=session.query(Order).filter(Order.filled==None,Order.buy_currency==order.sell_currency,
                                Order.sell_currency==order.buy_currency).all()
    result=[]
    if len(select)>0:
        for obj in select:
            #maker
            tmp_rate=obj.sell_amount / obj.buy_amount
            if tmp_rate>=cur_rate:
                result.append(obj)
    return result

def insert_order(order):
    obj = Order()
    for r in order.keys():
        obj.__setattr__(r, order[r])
    flag = True
    if obj.buy_currency == None or obj.buy_currency == '':
        print("buy_currency must not be null")
        flag = False
    if obj.sell_currency == None or obj.sell_currency == '':
        print("sell_currency must not be null")
        flag = False
    if obj.buy_amount == None or obj.buy_amount == '':
        print("buy_amount must not be null")
        flag = False
    if obj.sell_amount == None or obj.sell_amount == '':
        print("sell_amount must not be null")
        flag = False
    if obj.sender_pk == None or obj.sender_pk == '':
        print("sender_pk must not be null")
        flag = False
    if obj.receiver_pk == None or obj.receiver_pk == '':
        print("receiver_pk must not be null")
        flag = False
    if flag:
        session.add(obj)
        session.commit()
    else:
        obj=None
    return obj


def process_order(order):
    #Your code here
    target=insert_order(order)
    existing_order=None
    if target!=None:
        result=get_all_match_orders(target)
        if len(result)>0:
            sorted(result, key=lambda o: o.sell_amount, reverse=True)
            existing_order=result[0]
            #Set the filled field to be the current timestamp on both orders
            current_time=datetime.now()
            existing_order.filled=current_time
            target.filled=current_time
            #Set counterparty_id to be the id of the other order
            target.counterparty_id=existing_order.id
            existing_order.counterparty_id=target.id
            #Create a new order for remaining balance
            new_order = None
            if existing_order.buy_amount>target.sell_amount:
                new_order=Order()
                differ=existing_order.buy_amount - target.sell_amount
                new_order.buy_amount=differ
                sell_amount=differ*existing_order.sell_amount/existing_order.buy_amount
                new_order.sell_amount=sell_amount
                new_order.creator_id=existing_order.id
                new_order.sell_currency=existing_order.sell_currency
                new_order.buy_currency=existing_order.buy_currency
                new_order.receiver_pk=existing_order.receiver_pk
                new_order.sender_pk=existing_order.sender_pk
            if  existing_order.buy_amount<target.sell_amount:
                new_order = Order()
                differ = target.sell_amount - existing_order.buy_amount
                new_order.sell_amount = differ
                buy_amount = differ * target.buy_amount / target.sell_amount
                new_order.buy_amount = buy_amount
                new_order.creator_id = target.id
                new_order.sell_currency = target.sell_currency
                new_order.buy_currency = target.buy_currency
                new_order.receiver_pk = target.receiver_pk
                new_order.sender_pk = target.sender_pk
            if new_order!=None:
                session.add(new_order)
            session.commit()


