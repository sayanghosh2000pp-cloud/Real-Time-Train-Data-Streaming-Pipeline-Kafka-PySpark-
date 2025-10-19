
import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_json('sample_data/train_events.jsonl', lines=True)
agg = df.groupby('train_no')['delay_min'].mean().reset_index()
plt.bar(agg['train_no'], agg['delay_min'], color='skyblue')
plt.xlabel('Train Number')
plt.ylabel('Average Delay (min)')
plt.title('Average Delay per Train (Sample Data)')
plt.tight_layout()
plt.savefig('sample_output.png')
print('sample_output.png generated')
