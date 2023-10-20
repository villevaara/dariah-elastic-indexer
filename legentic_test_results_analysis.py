import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

leg_res_1 = pd.read_csv('test_results/legentic_bulk_size_test.csv')
leg_res_1['t_per_item'] = leg_res_1['avg_elapsed'] / leg_res_1['bulk_items']

leg_res_2 = pd.read_csv('test_results/legentic_bulk_size_test2.csv')
leg_res_2['t_per_item'] = leg_res_1['avg_elapsed'] / leg_res_1['bulk_items']

sns.barplot(x="t_per_item", y="avg_elapsed", data=leg_res_1, color="b"
            ).set(title='Legentic indexing times')
plt.savefig("plots/links_by_role.png", bbox_inches="tight", pad_inches=0.2)
plt.clf()
plt.close()
