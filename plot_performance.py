import pandas as pd
import matplotlib.pyplot as plt

# Load metrics for news article stream batches
metrics = pd.read_csv("news_metrics.csv")

plt.style.use('seaborn-v0_8-darkgrid')
fig, axes = plt.subplots(1, 2, figsize=(14, 5))

# Visualize throughput per batch
axes[0].plot(metrics['batch_idx'], metrics['throughput'], marker='s', color='navy', linewidth=2)
axes[0].set_title('News Stream Throughput by Batch')
axes[0].set_xlabel('Batch Index')
axes[0].set_ylabel('Articles/sec')
axes[0].grid(True, linestyle='--', alpha=0.5)

# Visualize average delay per batch
axes[1].plot(metrics['batch_idx'], metrics['avg_delay'], marker='^', color='crimson', linewidth=2)
axes[1].set_title('Average Ingestion Delay per Batch')
axes[1].set_xlabel('Batch Index')
axes[1].set_ylabel('Mean Delay (sec)')
axes[1].grid(True, linestyle=':', alpha=0.6)

plt.suptitle('News Article Stream Performance Metrics', fontsize=16, fontweight='bold')
plt.tight_layout(rect=[0, 0.03, 1, 0.95])
plt.savefig("news_stream_performance.png", dpi=150)
print("Performance plot saved as news_stream_performance.png")
# plt.show()  # Uncomment for interactive environments
