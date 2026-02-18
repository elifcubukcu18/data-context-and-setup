import pandas as pd
import numpy as np
from olist.utils import haversine_distance
from olist.data import Olist


class Order:
    '''
    DataFrames containing all orders as index,
    and various properties of these orders as columns
    '''
    def __init__(self):
        # Assign an attribute ".data" to all new instances of Order
        self.data = Olist().get_data()

    def get_wait_time(self, is_delivered=True):
        """
        Returns a DataFrame with:
        [order_id, wait_time, expected_wait_time, delay_vs_expected, order_status]
        and filters out non-delivered orders unless specified
        """
        # Hint: Within this instance method, you have access to the instance of the class Order in the variable self, as well as all its attributes
        olist = Olist()
        data = olist.get_data()
        orders = data['orders'].copy()
        orders[orders["order_status"] == "delivered"]
        orders['order_approved_at'] = pd.to_datetime(orders['order_approved_at'])
        orders['order_purchase_timestamp'] = pd.to_datetime(orders['order_purchase_timestamp'])
        orders['order_delivered_customer_date'] = pd.to_datetime(orders['order_delivered_customer_date'])
        orders['order_estimated_delivery_date'] = pd.to_datetime(orders['order_estimated_delivery_date'])
        orders['order_delivered_carrier_date'] = pd.to_datetime(orders['order_delivered_carrier_date'])
        orders["wait_time"] = (
        orders["order_delivered_customer_date"] -
        orders["order_purchase_timestamp"]
            ) / pd.Timedelta(days=1)

        orders["expected_wait_time"] = (
        orders["order_estimated_delivery_date"] -
        orders["order_purchase_timestamp"]
                ) / pd.Timedelta(days=1)

        orders["delay_vs_expected"] = (
        (orders["order_delivered_customer_date"] -
        orders["order_estimated_delivery_date"])
            / pd.Timedelta(days=1)
)               .clip(lower=0)
        if is_delivered:
                orders = orders[orders["order_status"] == "delivered"]

        return orders[[
         "order_id",
         "wait_time",
        "expected_wait_time",
        "delay_vs_expected",
        "order_status"
        ]]


    def get_review_score(self):
        """
        Returns a DataFrame with:
        order_id, dim_is_five_star, dim_is_one_star, review_score
        """
        olist = Olist()
        data = olist.get_data()
        reviews = data['order_reviews'].copy()
        reviews["dim_is_five_star"] = (reviews["review_score"] == 5).astype(int)
        reviews["dim_is_one_star"] = (reviews["review_score"] == 1).astype(int)
        reviews.drop(columns=['review_id', 'review_comment_title', 'review_comment_message', 'review_creation_date', 'review_answer_timestamp'], inplace=True)
        return reviews



    def get_number_items(self):
        """
        Returns a DataFrame with:
        order_id, number_of_items
        """
        from olist.data import Olist
        olist = Olist()
        data = olist.get_data()
        order_item2 = data['order_items']

        order_item2['number_of_items'] = order_item2.groupby('order_id')['order_item_id'].transform('count')
        return order_item2[['order_id', 'number_of_items']].drop_duplicates()


    def get_number_sellers(self):
        """
        Returns a DataFrame with:
        order_id, number_of_sellers
        """
        from olist.data import Olist
        olist = Olist()
        data = olist.get_data()
        order_item = data['order_items']
        orders = data['orders'].copy()

        oi = order_item.merge(
        orders[["order_id"]],
        on="order_id",
        how="inner"
            )


        oi = oi.dropna(subset=["seller_id"])


        number_sellers = (
            oi.groupby("order_id")["seller_id"]
            .nunique()
            .reset_index(name="number_of_sellers")
            )
        return number_sellers


    def get_price_and_freight(self):
        """
        Returns a DataFrame with:
        order_id, price, freight_value
        """
        from olist.data import Olist
        olist = Olist()
        data = olist.get_data()
        order_item = data['order_items']
        order_item.head()

        order_item['price'] = order_item.groupby('order_id')['price'].transform('sum')
        order_item['freight_value'] = order_item.groupby('order_id')['freight_value'].transform('sum')

        return order_item[['order_id', 'price', 'freight_value']].drop_duplicates()

    # Optional
    def get_distance_seller_customer(self):
        """
        Returns a DataFrame with:
        order_id, distance_seller_customer
        """
        pass  # YOUR CODE HERE

    def get_training_data(self,
                          is_delivered=True,
                          with_distance_seller_customer=False):
        """
        Returns a clean DataFrame (without NaN), with the all following columns:
        ['order_id', 'wait_time', 'expected_wait_time', 'delay_vs_expected',
        'order_status', 'dim_is_five_star', 'dim_is_one_star', 'review_score',
        'number_of_items', 'number_of_sellers', 'price', 'freight_value',
        'distance_seller_customer']
        """
        # Hint: make sure to re-use your instance methods defined above, and to merge on "order_id"
        wait = self.get_wait_time(is_delivered=False)   # order_id, wait_time, expected_wait_time, delay_vs_expected, order_status
        rev  = self.get_review_score()                  # order_id, review_score, dim_is_five_star, dim_is_one_star
        price = self.get_price_and_freight()                        # order_id, price, freight_value
        items = self.get_number_items()                 # order_id, number_of_items
        sellers = self.get_number_sellers()             # order_id, number_of_sellers

        df = (wait
            .merge(rev, on="order_id", how="left")
            .merge(items, on="order_id", how="left")
            .merge(sellers, on="order_id", how="left")
            .merge(price, on="order_id", how="left"))

        df = df[df["order_status"] == "delivered"]  # is_delivered True ise
        df = df.dropna()
        df = df[[
            "delay_vs_expected","dim_is_five_star","dim_is_one_star","expected_wait_time",
            "freight_value","number_of_items","number_of_sellers","order_id","order_status",
            "price","review_score","wait_time"
                ]]

        return df
