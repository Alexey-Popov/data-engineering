Use Case 2: Amazon Reviews

Some time ago, Amazon open-sourced some interesting datasets that can be used for Data Science experiments. One such dataset is a collection of product reviews. It contains a large number of real reviews made by customers over some period of time.

We will be using this dataset for the next few homeworks.

The dataset contains the following columns:

marketplace - country code for the review

customer_id - id of the customer, who left the review

review_id - unique review

id product_id - unique id of the product, for which review is provided

product_parent - id of the parent product (if applicable)

product_title - title of the product

product_category - category of the product

star_rating - rating given to the product in the scope of this review

helpful_votes - number of votes for this review by other customers

total_votes - total number of votes

vine - (this one is not clear)

verified_purchase - the flag if the purchasee is verified (customer bought this product)

review_headline - short headline for the review

review_body - full body of the review

review_date - review date