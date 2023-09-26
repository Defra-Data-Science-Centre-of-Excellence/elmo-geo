from elmo_geo.st.index import index
from elmo_geo.st.join import join


# def groupby(sdf, keys:list=None, mapper:dict=None, col:str='geometry'):
# 	return (sdf
# 		.groupby(keys)
# 		.agg({
# 			F.expr('ST_Union_Aggr({col}) AS {col}'),
# 			**mapper
# 		})
# 	)
