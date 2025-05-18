from flask import Flask, jsonify, render_template
from test_main import main_beta, calculate_beta, calculate_trend_usdt, calculate_trend_btc, calculate_trend_sol, calculate_trend_sui, calculate_trend_eth
from logging_config import setup_logger
from waitress import serve  # ✅ New import
import atexit
from tournament_trend_calculator import main as shitcoin_tournament
logger = setup_logger(__name__)

# Log that the Flask app is starting
logger.info("Flask app starting up...")

app = Flask(__name__)

# Register exit function to log when the app shuts down
def log_exit():
    logger.info("Flask app is shutting down...")

atexit.register(log_exit)

@app.route('/')
def home():
    """Serve the home page with links to all endpoints."""
    return render_template('index.html')

@app.route('/calculate-full-system', methods=['GET', 'POST'])
def run_full_system_endpoint():
    try:
        logger.info("Starting beta and trends calculation")
        token_trend_scores_list = main_beta()
        logger.info("Beta and trends calculation completed")
        return jsonify({"status": "success", 
                        "message": "Beta scores calculated and written to Google Sheets. Trend for TOKEN/USDT Calculated and writen to Google Sheets",
                        "Results": token_trend_scores_list}), 200
    except Exception as e:
        logger.error(f"Error during beta calculation: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/calculate-beta', methods=['GET', 'POST'])
def run_beta_endpoint():
    try:
        logger.info("Starting beta calculation")
        beta_scores_list, skipped_coins = calculate_beta()
        logger.info("Beta calculation completed")
        return jsonify({
            "status": "success",
            "message": "Beta scores calculated and written to Google Sheets",
            "results": beta_scores_list,
            "skipped_coins": skipped_coins
        }), 200
    except Exception as e:
        logger.error(f"Error during beta calculation: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/calculate-trend-usdt', methods=['GET', 'POST'])
def run_trend_usdt_endpoint():
    try:
        logger.info("Starting TOKEN/USDT trend calculation")
        trend_scores_list, skipped_coins = calculate_trend_usdt()
        logger.info("TOKEN/USDT trend calculation completed")
        return jsonify({
            "status": "success",
            "message": "TOKEN/USDT trend scores calculated and written to Google Sheets",
            "results": trend_scores_list,
            "skipped_coins": skipped_coins
        }), 200
    except Exception as e:
        logger.error(f"Error during TOKEN/USDT trend calculation: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/calculate-trend-btc', methods=['GET', 'POST'])
def run_trend_btc_endpoint():
    try:
        logger.info("Starting TOKEN/BTC trend calculation")
        trend_scores_list_with_names, trend_scores_list, skipped_coins = calculate_trend_btc()
        logger.info("TOKEN/BTC trend calculation completed")
        return jsonify({
            "status": "success",
            "message": "TOKEN/BTC trend scores calculated and written to Google Sheets",
            "results": trend_scores_list_with_names,
            "skipped_coins": skipped_coins
        }), 200
    except Exception as e:
        logger.error(f"Error during TOKENanee/BTC trend calculation: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/calculate-trend-sol', methods=['GET', 'POST'])
def run_trend_sol_endpoint():
    try:
        logger.info("Starting TOKEN/SOL trend calculation")
        trend_scores_list_with_names, trend_scores_list, skipped_coins = calculate_trend_sol()
        logger.info("TOKEN/SOL trend calculation completed")
        return jsonify({
            "status": "success",
            "message": "TOKEN/SOL trend scores calculated and written to Google Sheets",
            "results": trend_scores_list_with_names,
            "skipped_coins": skipped_coins
        }), 200
    except Exception as e:
        logger.error(f"Error during TOKEN/SOL trend calculation: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/calculate-trend-sui', methods=['GET', 'POST'])
def run_trend_sui_endpoint():
    try:
        logger.info("Starting TOKEN/SUI trend calculation")
        trend_scores_list_with_names, trend_scores_list, skipped_coins = calculate_trend_sui()
        logger.info("TOKEN/SUI trend calculation completed")
        return jsonify({
            "status": "success",
            "message": "TOKEN/SUI trend scores calculated and written to Google Sheets",
            "results": trend_scores_list_with_names,
            "skipped_coins": skipped_coins
        }), 200
    except Exception as e:
        logger.error(f"Error during TOKEN/SUI trend calculation: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/calculate-trend-eth', methods=['GET', 'POST'])
def run_trend_eth_endpoint():
    try:
        logger.info("Starting TOKEN/ETH trend calculation")
        trend_scores_list_with_names, trend_scores_list, skipped_coins = calculate_trend_eth()
        logger.info("TOKEN/ETH trend calculation completed")
        return jsonify({
            "status": "success",
            "message": "TOKEN/ETH trend scores calculated and written to Google Sheets",
            "results": trend_scores_list_with_names,
            "skipped_coins": skipped_coins
        }), 200
    except Exception as e:
        logger.error(f"Error during TOKEN/ETH trend calculation: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/calculate_shitcoins_tournament', methods=['GET', 'POST'])
def run_tournament_shitcoins_endpoint():
    try:
        logger.info("Starting Shitcoins Tournament")
        response = shitcoin_tournament()
        logger.info("Shitcoins Tournament calculation completed")
        return jsonify({
            "status": "success",
            "message": response,
        }), 200
    except Exception as e:
        logger.error(f"Error during TOKEN/ETH trend calculation: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    logger.info("Flask app running on http://127.0.0.1:8080")
    logger.info("Endpoints:")
    logger.info("http://127.0.0.1:8080/calculate-beta")
    logger.info("http://127.0.0.1:8080/calculate-trend-usdt")
    logger.info("http://127.0.0.1:8080/calculate-trend-btc")
    logger.info("http://127.0.0.1:8080/calculate-trend-sol")
    logger.info("http://127.0.0.1:8080/calculate-trend-sui")
    logger.info("http://127.0.0.1:8080/calculate-trend-eth")
    logger.info("http://127.0.0.1:8080/calculate-full-system")
    logger.info("http://127.0.0.1:8080/calculate_shitcoins_tournament")
    
    
    serve(app, host="0.0.0.0", port=8080)