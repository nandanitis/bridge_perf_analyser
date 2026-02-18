
# Currently Normalising only ACQ DF : 
def acqdf_normaliser(acq_df,logger):
    logger.debug("Converting ACQ DR row values to float, so that we can plot the graphs in the next function")
    acq_df['busy_percent'] = acq_df['busy_percent'].astype(float)
    return acq_df