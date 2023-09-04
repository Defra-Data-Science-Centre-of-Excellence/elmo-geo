from .st.index import index  # default indexing method
from .st.join import join  # default join method


def groupby(sdf, keys:list=None, map:dict=None, col:str='geometry'):
	return (sdf
		.groupby(keys)
		.agg({
			F.expr('ST_Union_Aggr({col}) AS {col}'),
			**map
		})
	)
