library(arrow)
library(sfarrow)

dbfs = \(res) paste0('/dbfs/mnt/lab/unrestricted/elm/buffer_strips/', res, '_wfm.parquet')
data = \(res) paste0('data/', res, '_wfm.rds')
read = \(f, sp) if (sp) {st_read_parquet(f)} else {read_parquet(f)}
for (res in c('100km', '10km', '1km', 'ha', 'parcel')) {
  print(res)
  sp = !res %in% c('1km', 'ha', 'parcel')
  df = read(dbfs(res), sp)
  saveRDS(df, data(res))
}
rm(list = ls())
