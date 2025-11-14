from flask import Flask, jsonify, request
import ai_analysis as analysis

app = Flask(__name__)


@app.route("/", methods=["GET"])
def home():
    """Root endpoint."""
    return jsonify({
        "message": "Welcome to the AI Job Risk API!",
        "routes": [
            "/api/high-risk-jobs",
            "/api/industry-summary"
        ]
    })


@app.route("/api/high-risk-jobs", methods=["GET"])
def high_risk_jobs():
    """Return list of jobs most likely to be automated."""
    df = analysis.load_data()
    if df.empty:
        return jsonify({"error": "Dataset not found"}), 500

    risk_threshold = int(request.args.get("risk_threshold", 80))
    top_n = int(request.args.get("top", 10))

    risky = analysis.filter_high_risk_jobs(df, risk_threshold)
    top_jobs = analysis.top_high_risk_jobs(risky, top_n)

    return jsonify({
        "total_jobs_analyzed": len(df),
        "high_risk_jobs_found": len(risky),
        "top_jobs": top_jobs.to_dict(orient="records")
    })


@app.route("/api/industry-summary", methods=["GET"])
def industry_summary():
    """Return summary of risky jobs by industry."""
    df = analysis.load_data()
    if df.empty:
        return jsonify({"error": "Dataset not found"}), 500

    risky = analysis.filter_high_risk_jobs(df)
    summary = analysis.summarize_industry_risk(risky)

    return jsonify({
        "total_industries": len(summary),
        "industry_summary": summary
    })


if __name__ == "__main__":
    print("[INFO] Starting Flask server...")
    app.run(host="0.0.0.0", port=5000, debug=True)
