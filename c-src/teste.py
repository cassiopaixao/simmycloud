#!/usr/bin/env python3.4

import rbf

a = [0.00176, 
0.00173    , 
0.001732   , 
0.001766   , 
0.001747   , 
0.001772   , 
0.001715   , 
0.001719   , 
0.001757   , 
0.001741   , 
0.001743   , 
0.001759   , 
0.001781   , 
0.001717   , 
0.001717   , 
0.001738   , 
0.001715   , 
0.00173    , 
0.001722   , 
0.00172    , 
0.00173    , 
0.001713   , 
0.001711   , 
0.00172    , 
0.001717   , 
0.001711   , 
0.00172    , 
0.001728   , 
0.001726   , 
0.00173    , 
0.001717   , 
0.001713   , 
0.001717   , 
0.001726   , 
0.001717   , 
0.001703   , 
0.001719   , 
0.001732   , 
0.001722   , 
0.001734   , 
0.001816   , 
0.001766   , 
0.001741   , 
0.001759   , 
0.001745   , 
0.001764   , 
0.00174    , 
0.001759   , 
0.001747   , 
0.001759]

print(rbf.predict(a))

a = [0.0] * 50
print(rbf.predict(a))


