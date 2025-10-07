Goal:
Task is to use a multi-class approach to predict the the outcomes of patients with cirrhosis.

For each id row in the test set, i must predict probabilities of the three outcomes Status_C, Status_CL, and Status_D.


train.csv - the training dataset; Status is the categorical target; 

C (censored) indicates the patient was alive at N_Days, 

CL indicates the patient was alive at N_Days due to liver a transplant, 

D indicates the patient was deceased at N_Days.


test.csv - the test dataset. 

My objective is to predict the probability of each of the three Status values, e.g., Status_C, Status_CL, Status_D.
