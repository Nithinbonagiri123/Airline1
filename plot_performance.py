import pandas as pd
import matplotlib.pyplot as plt
from consumer.s3_upload_helper import upload_file_to_s3

# Load performance data for airline customer review streaming pipeline
perf_data = pd.read_csv("airline_customer_review_metrics.csv")

plt.style.use("ggplot")
fig, plots = plt.subplots(1, 2, figsize=(13, 5))

# Plot streaming throughput
plots[0].plot(
    perf_data["batch_num"],
    perf_data["throughput"],
    marker="o",
    color="teal",
    linewidth=2,
)
plots[0].set_title("Batch-wise Stream Throughput")
plots[0].set_xlabel("Batch Number")
plots[0].set_ylabel("Records per Second")
plots[0].grid(True, linestyle="-.", alpha=0.7)

# Plot average ingestion delay
plots[1].plot(
    perf_data["batch_num"],
    perf_data["avg_latency"],
    marker="x",
    color="orange",
    linewidth=2,
)
plots[1].set_title("Mean Ingestion Delay per Batch")
plots[1].set_xlabel("Batch Number")
plots[1].set_ylabel("Average Delay (seconds)")
plots[1].grid(True, linestyle=":", alpha=0.5)

plt.suptitle(
    "Airline Customer Review Streaming Pipeline: Performance Overview",
    fontsize=15,
    fontweight="bold",
)
plt.tight_layout(rect=[0, 0.03, 1, 0.95])
plt.savefig("stream_performance_summary.png", dpi=150)
print("Saved performance summary as stream_performance_summary.png")
upload_file_to_s3(
    "stream_performance_summary.png",
    "your-bucket-name",
    "stream_performance_summary.png",
)
# plt.show()  # Uncomment for interactive review
