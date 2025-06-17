import random
import string
from time import time
from client_utils import send_batch_commands_new_socket


def random_name(length=6):
    return "".join(random.choices(string.ascii_lowercase, k=length))


def send_single_command_wrapper(command: str) -> str:
    responses = send_batch_commands_new_socket([command])
    if responses:
        return responses[0]
    return "Error: No response received."


create_db_response = send_single_command_wrapper("CREATE DATABASE nagyABindex")
print(f"CREATE DATABASE nagyABindex: {create_db_response}")

use_response = send_single_command_wrapper("USE nagyABindex")
print(f"USE nagyABindex   : {use_response}")

print(send_single_command_wrapper("CREATE TABLE users id:int name:str age:int salary:float"))
print(send_single_command_wrapper("CREATE TABLE products product_id:int product_name:str price:int category_id:int brand_id:int"))
print(send_single_command_wrapper("CREATE TABLE categories category_id:int category_name:str"))
print(send_single_command_wrapper("CREATE TABLE brands brand_id:int brand_name:str"))
print(send_single_command_wrapper("CREATE TABLE orders order_id:int user_id:int order_date:str total_amount:float status:str"))

num_categories = 100
num_brands = 50

print(f"\n Generating {num_categories} categories and {num_brands} brands ")
category_insert_commands = [
    f'INSERT INTO categories VALUES ({i} "category_{i}")'
    for i in range(1, num_categories + 1)
]
brand_insert_commands = [
    f'INSERT INTO brands VALUES ({i} "brand_{i}")'
    for i in range(1, num_brands + 1)
]

category_bulk_command = f"INSERT BULK INTO categories VALUES {', '.join([c.split('VALUES ')[1] for c in category_insert_commands])}"
brand_bulk_command = f"INSERT BULK INTO brands VALUES {', '.join([b.split('VALUES ')[1] for b in brand_insert_commands])}"

print(
    f"Inserting {len(category_insert_commands)} categories in one bulk operation "
)
print(send_single_command_wrapper(category_bulk_command))
print(
    f"Inserting {len(brand_insert_commands)} brands in one bulk operation "
)
print(send_single_command_wrapper(brand_bulk_command))
print("Categories and Brands inserted.")


num_users = 100000

print(f"\n Generating {num_users} users with bulk insert ")
start_time_users = time()

batch_size_users = 10000

for i in range(0, num_users, batch_size_users):
    batch_records = []
    for j in range(batch_size_users):
        user_id = i + j
        if user_id >= num_users:
            break
        name = random_name()
        age = random.randint(18, 100)
        salary = random.randint(1000, 10000)
        batch_records.append(f'({user_id} "{name}" {age} {salary})')

    if not batch_records:
        continue

    bulk_command = f"INSERT BULK INTO users VALUES {', '.join(batch_records)}"
    response = send_single_command_wrapper(bulk_command)

    if "Error" in response:
        print(f"Error inserting batch starting with ID {i}: {response}")

    if (i // batch_size_users) % 10 == 0:
        print(f"Inserted {i + len(batch_records)} users.")

print(f"Finished inserting {num_users} users.")


num_products = 100000

print(f"\n Generating {num_products} products with bulk insert ")
start_time_products = time()

batch_size_products = 10000

for i in range(0, num_products, batch_size_products):
    batch_records = []
    for j in range(batch_size_products):
        product_id = i + j
        if product_id >= num_products:
            break
        product_name = f"product_{product_id}"
        price = random.randint(50, 500)
        category_id = random.randint(1, num_categories)
        brand_id = random.randint(1, num_brands)
        batch_records.append(
            f'({product_id} "{product_name}" {price} {category_id} {brand_id})'
        )

    if not batch_records:
        continue

    bulk_command = (
        f"INSERT BULK INTO products VALUES {', '.join(batch_records)}"
    )
    response = send_single_command_wrapper(bulk_command)

    if "Error" in response:
        print(
            f"Error inserting product batch starting with ID {i}: {response}"
        )

    if (i // batch_size_products) % 10 == 0:
        print(f"Inserted {i + len(batch_records)} products.")

print(f"Finished inserting {num_products} products")


num_orders = 100000
print(f"\n Generating {num_orders} orders with bulk insert ")
start_time_orders = time()

batch_size_orders = 10000
order_statuses = ["shipped", "pending", "delivered", "cancelled"]

for i in range(0, num_orders, batch_size_orders):
    batch_records = []
    for j in range(batch_size_orders):
        order_id = i + j
        if order_id >= num_orders:
            break
    
        user_id = random.randint(0, num_users - 1)
        order_date = f"2024-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}"
        total_amount = random.randint(20, 500)
        status = random.choice(order_statuses)
        batch_records.append(
            f'({order_id} {user_id} "{order_date}" {total_amount} "{status}")'
        )

    if not batch_records:
        continue

    bulk_command = f"INSERT BULK INTO orders VALUES {', '.join(batch_records)}"
    response = send_single_command_wrapper(bulk_command)

    if "Error" in response:
        print(f"Error inserting order batch starting with ID {i}: {response}")

    if (i // batch_size_orders) % 10 == 0:
        print(f"Inserted {i + len(batch_records)} orders.")

print(f"Finished inserting {num_orders} orders")


print("\n All generation done. Creating indexes...")
print(send_single_command_wrapper("CREATE INDEX category_id ON products"))
print(send_single_command_wrapper("CREATE INDEX brand_id ON products"))

print(send_single_command_wrapper("CREATE INDEX user_id ON orders"))
print("\nGeneration complete!")