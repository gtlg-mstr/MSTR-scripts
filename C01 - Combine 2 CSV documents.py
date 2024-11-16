#!/usr/bin/env python
# coding: utf-8

import pandas as pd

qalist = pd.read_csv('/Users/glagrange/Library/CloudStorage/OneDrive-MicroStrategy,Inc/QA-dossiers_viz.csv')
prodlist = pd.read_csv('/Users/glagrange/Library/CloudStorage/OneDrive-MicroStrategy,Inc/PROD_dossiers_viz.csv')

display (qalist)
print("")
display(prodlist)

frames = [qalist, prodlist]

result = pd.concat(frames)
result.to_csv('/Users/glagrange/Library/CloudStorage/OneDrive-MicroStrategy,Inc/merged_dossier_viz.csv')
