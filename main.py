from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import psycopg2
from azureml.core import Workspace, Model
import pandas as pd

app = FastAPI()

# Configuración de la base de datos PostgreSQL
DATABASE_CONFIG = {
    'dbname': 'public',
    'user': 'wsuarez',
    'password': 'Afsmnz78',
    'host': 'ventas-cobranzas-db.postgres.database.azure.com',
    'port': '5432'
}

# Configuración de Azure Machine Learning
AZURE_ML_CONFIG = {
    'subscription_id': 'b67f950b-3f32-46d1-891e-075ca3772ec3',
    'resource_group': 'ventas-cobranzas-rg',
    'workspace_name': 'ventas-cobranzas-ia'
}

# Conexión a la base de datos
def get_db_connection():
    conn = psycopg2.connect(**DATABASE_CONFIG)
    return conn

# Cargar modelo de Azure ML
def load_azure_ml_model():
    ws = Workspace(**AZURE_ML_CONFIG)
    model = Model(ws, name='your_model_name')
    return model

# Endpoint para obtener productos más vendidos
@app.get("/top_products")
async def get_top_products():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT product_id, COUNT(*) as sales FROM sales GROUP BY product_id ORDER BY sales DESC LIMIT 10")
    top_products = cursor.fetchall()
    conn.close()
    return {"top_products": top_products}

# Endpoint para obtener el top de ciudades con más ventas
@app.get("/top_cities")
async def get_top_cities():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT city, COUNT(*) as sales FROM sales GROUP BY city ORDER BY sales DESC LIMIT 10")
    top_cities = cursor.fetchall()
    conn.close()
    return {"top_cities": top_cities}

# Endpoint para obtener categorías de productos más vendidos
@app.get("/top_categories")
async def get_top_categories():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT category, COUNT(*) as sales FROM sales GROUP BY category ORDER BY sales DESC LIMIT 10")
    top_categories = cursor.fetchall()
    conn.close()
    return {"top_categories": top_categories}

# Endpoint para obtener reporte de compras de un cliente por fechas
@app.get("/customer_purchases/{customer_id}")
async def get_customer_purchases(customer_id: int, start_date: str, end_date: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM sales WHERE customer_id = {customer_id} AND sale_date BETWEEN '{start_date}' AND '{end_date}'")
    purchases = cursor.fetchall()
    conn.close()
    return {"purchases": purchases}

# Endpoint para predecir si se le puede vender a un cliente
@app.get("/predict_sale/{customer_id}")
async def predict_sale(customer_id: int):
    model = load_azure_ml_model()
    # Aquí deberías obtener los datos del cliente y preprocesarlos para la predicción
    # Ejemplo: customer_data = get_customer_data(customer_id)
    # prediction = model.predict(customer_data)
    # return {"prediction": prediction}
    return {"prediction": "Not implemented yet"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
