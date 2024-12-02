import pymysql
from pymongo import MongoClient

# Conexión a MySQL
mysql_conn = pymysql.connect(
    host="localhost",
    user="root",
    password="12345",
    database="Northwind"
)

# Conexión a MongoDB
client = MongoClient("mongodb+srv://root:12345@clustergratis.uw31r.mongodb.net/")
db = client["NorthwindEsquema2"]

def migrate_schema2():
    try:
        with mysql_conn.cursor() as cursor:
            # Migrar Orders con detalles anidados
            cursor.execute("SELECT OrderID, CustomerID, EmployeeID, OrderDate, ShipperID FROM Orders")
            orders = cursor.fetchall()
            db["Orders"].delete_many({})
            for order in orders:
                order_id, customer_id, employee_id, order_date, shipper_id = order

                # Obtener detalles de la orden
                cursor.execute("SELECT ProductID, Quantity FROM OrderDetails WHERE OrderID = %s", (order_id,))
                order_details = cursor.fetchall()

                # Formatear datos
                db["Orders"].insert_one({
                    "OrderID": order_id,
                    "CustomerID": customer_id,
                    "EmployeeID": employee_id,
                    "OrderDate": str(order_date),
                    "ShipperID": shipper_id,
                    "OrderDetails": [
                        {"ProductID": detail[0], "Quantity": detail[1]} for detail in order_details
                    ]
                })

            # Migrar Products
            cursor.execute("SELECT ProductID, ProductName, SupplierID, CategoryID, Unit, Price FROM Products")
            products = cursor.fetchall()
            db["Products"].delete_many({})
            db["Products"].insert_many(
                [{"ProductID": row[0], "ProductName": row[1], "SupplierID": row[2],
                  "CategoryID": row[3], "Unit": row[4], "Price": float(row[5])} for row in products]
            )
            
            print("Migración del esquema 2 completada.")
    except Exception as e:
        print(f"Error durante la migración del esquema 2: {e}")

if __name__ == "__main__":
    migrate_schema2()
    client.close()
    mysql_conn.close()
