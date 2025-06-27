import pandas as pd
import matplotlib.pyplot as plt

# Load the performance results CSV
df = pd.read_csv("performance_results.csv")

plt.figure(figsize=(12, 5))

# Plot Throughput per Batch
plt.subplot(1, 2, 1)
plt.plot(df['epoch_id'], df['throughput'], marker='o')
plt.title('Throughput per Batch')
plt.xlabel('Batch (epoch_id)')
plt.ylabel('Throughput (messages/sec)')

# Plot Average Latency per Batch
plt.subplot(1, 2, 2)
plt.plot(df['epoch_id'], df['avg_latency'], marker='o', color='red')
plt.title('Average Latency per Batch')
plt.xlabel('Batch (epoch_id)')
plt.ylabel('Average Latency (sec)')

plt.tight_layout()
plt.savefig("performance_plot.png")
print("Plot saved as performance_plot.png")
# plt.show()  # Commented out for non-interactive environments
