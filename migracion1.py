import pymysql
from pymongo import MongoClient


mysql_conn = pymysql.connect(
    host="localhost",
    user="root",
    password="12345",
    database="Northwind"
)


client = MongoClient("mongodb+srv://root:12345@clustergratis.uw31r.mongodb.net/")
db = client["NorthwindEsquema1"]

def migrate_schema1():
    try:
        with mysql_conn.cursor() as cursor:
            
            cursor.execute("SELECT CategoryID, CategoryName, Description FROM Categories")
            categories = cursor.fetchall()
            db["Categories"].delete_many({})
            db["Categories"].insert_many(
                [{"CategoryID": row[0], "CategoryName": row[1], "Description": row[2]} for row in categories]
            )

            
            cursor.execute("SELECT CustomerID, CustomerName, ContactName, Address, City, PostalCode, Country FROM Customers")
            customers = cursor.fetchall()
            db["Customers"].delete_many({})
            db["Customers"].insert_many(
                [{"CustomerID": row[0], "CustomerName": row[1], "ContactName": row[2],
                  "Address": row[3], "City": row[4], "PostalCode": row[5], "Country": row[6]} for row in customers]
            )

            # Migrar Employees
            cursor.execute("SELECT EmployeeID, LastName, FirstName, BirthDate, Photo, Notes FROM Employees")
            employees = cursor.fetchall()
            db["Employees"].delete_many({})
            db["Employees"].insert_many(
                [{"EmployeeID": row[0], "LastName": row[1], "FirstName": row[2],
                  "BirthDate": str(row[3]), "Photo": row[4], "Notes": row[5]} for row in employees]
            )

            # Migrar Suppliers
            cursor.execute("SELECT SupplierID, SupplierName, ContactName, Address, City, PostalCode, Country, Phone FROM Suppliers")
            suppliers = cursor.fetchall()
            db["Suppliers"].delete_many({})
            db["Suppliers"].insert_many(
                [{"SupplierID": row[0], "SupplierName": row[1], "ContactName": row[2], "Address": row[3],
                  "City": row[4], "PostalCode": row[5], "Country": row[6], "Phone": row[7]} for row in suppliers]
            )

            # Migrar Products
            cursor.execute("SELECT ProductID, ProductName, SupplierID, CategoryID, Unit, Price FROM Products")
            products = cursor.fetchall()
            db["Products"].delete_many({})
            db["Products"].insert_many(
                [{"ProductID": row[0], "ProductName": row[1], "SupplierID": row[2],
                  "CategoryID": row[3], "Unit": row[4], "Price": float(row[5])} for row in products]
            )
            
            # Migrar Orders
            cursor.execute("SELECT OrderID, CustomerID, EmployeeID, OrderDate, ShipperID FROM Orders")
            orders = cursor.fetchall()
            db["Orders"].delete_many({})
            db["Orders"].insert_many(
                [{"OrderID": row[0], "CustomerID": row[1], "EmployeeID": row[2],
                  "OrderDate": str(row[3]), "ShipperID": row[4]} for row in orders]
            )
            
            print("Migración del esquema 1 completada.")
    except Exception as e:
        print(f"Error durante la migración del esquema 1: {e}")

if __name__ == "__main__":
    migrate_schema1()
    client.close()
    mysql_conn.close()
