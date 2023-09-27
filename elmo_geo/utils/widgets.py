from elmo_geo.utils.dbr import dbutils
from functools import partial



def get_widget(name:str, choices:list[str]) -> list[str]:
  result = dbutils.widgets.get(name).split(',')
  if 'ALL' in result:
    result = choices
  return result


def create_widget(name:str, choices:list[str]) -> callable:
  dbutils.widgets.multiselect('x', 'ALL', ['ALL', *choices])
  return partial(get_widget, name=name, choices=choices)



if __name__ == '__main__':
  get_x = create_widget('x', ['A', 'B', 'C'])
  get_x()
