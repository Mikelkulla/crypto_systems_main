from flask import Flask, jsonify
from test_main import main_beta
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

@app.route('/calculate-beta-tokenusdt', methods=['GET', 'POST'])
def run_beta_endpoint():
    try:
        logger.info("Starting beta calculation")
        main_beta()
        logger.info("Beta calculation completed")
        return jsonify({"status": "success", "message": "Beta scores calculated and written to Google Sheets. Trend for TOKEN/USDT Calculated and writen to Google Sheets"}), 200
    except Exception as e:
        logger.error(f"Error during beta calculation: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)