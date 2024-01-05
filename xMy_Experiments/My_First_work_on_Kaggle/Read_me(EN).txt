Goal:
Predict defects in C programs given various various attributes about the code.

For each id in the test set, i must predict the probability for the target variable defects.


But I did not correctly understand the purpose of the task 
and at the output my model simply produced a binary value for the classes, instead of a probability. 
This version has already been fixed in the last line of my code, but I did not have time to do this on the kagle, 
because I downloaded the solution at the very last moment and did not have time to correct it there, which I greatly regret, 
because I would be interested to see how my approach to choosing features would affect the final result.

Anyway:

The dataset was generated from a deep learning model trained on the Software Defect Dataset.

train.csv - the training dataset; defects is the binary target, which is treated as a boolean (False=0, True=1)

test.csv - the test dataset; my objective is to predict the probability of positive defects (i.e., defects=True)