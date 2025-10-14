> 🇷🇺 [Читать на русском](README.md)

## 🗂️ Navigation:

| File | Description |
|------|------------|
| **[My_Decision.ipynb](My_Decision.ipynb)** | **Goal:** Use a multi-class approach to predict outcomes of patients with liver cirrhosis. For each identifier in the test set, predict the probabilities of three outcomes: `Status_C`, `Status_CL`, and `Status_D`. |

ℹ️
| **Input Data** | **Description** |
|------|------------|
| **train.csv** | Training dataset. The **Status** column is the categorical target variable: <br>• **C (censored)** — patient was alive for `N_Days`; <br>• **CL** — patient was alive for `N_Days` due to liver transplant; <br>• **D** — patient died within `N_Days`. |
| **test.csv** | Test dataset used to predict the probability of each of the three status values (`Status_C`, `Status_CL`, `Status_D`). |

---
