from flask import Flask, jsonify
from test_main import main_beta
from logging_config import setup_logger
from waitress import serve  # ✅ New import
import atexit

logger = setup_logger(__name__)

# Log that the Flask app is starting
logger.info("Flask app starting up...")

app = Flask(__name__)

# Register exit function to log when the app shuts down
def log_exit():
    logger.info("Flask app is shutting down...")

atexit.register(log_exit)

@app.route('/calculate-beta-tokenusdt', methods=['GET', 'POST'])
def run_beta_endpoint():
    try:
        logger.info("Starting beta and trends calculation")
        main_beta()
        logger.info("Beta and trends calculation completed")
        return jsonify({"status": "success", "message": "Beta scores calculated and written to Google Sheets. Trend for TOKEN/USDT Calculated and writen to Google Sheets"}), 200
    except Exception as e:
        logger.error(f"Error during beta calculation: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    # ✅ Run with Waitress instead of Flask's development server
    logger.info("Flask app running on http://127.0.0.1:8080")
    logger.info("Endpoints:")
    logger.info("http://127.0.0.1:8080/calculate-beta-tokenusdt")
    
    serve(app, host="0.0.0.0", port=8080)
    # app.run(host="0.0.0.0", port=8080)