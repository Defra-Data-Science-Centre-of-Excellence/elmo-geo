library(magrittr)
library(sf)
library(shiny)
library(leaflet)
library(leaflet.extras)
library(leafpop)


{
  key <- ''  # os key

  res <- {list(
    `100km²` = readRDS('data/100km_wfm.rds'),
    `10km²` = readRDS('data/10km_wfm.rds'),
    `1km²` = 'data/1km_wfm.rds',
    `ha` = 'data/ha_wfm.rds',
    `parcel` = 'data/parcel_wfm.rds'
  )}

  pal0 <- colorNumeric('Greens', NULL, '#FFFFFF00')
  pal <- function(x) mapply(
    function(y) adjustcolor(pal0(y), alpha.f=sqrt(y)),
    scales::rescale(x, from=range(x))
  )
}


ui <- {fluidPage(
  tags$style(type='text/css', '
    #map{height:100vh!important}
    .container-fluid{padding:0}
    .leaflet-popup-content-wrapper{overflow:auto;max-height:calc(min(400px, 60vh))}
  '),
  leafletOutput('map'),
  absolutePanel(top=10, right=10,
    selectInput('res', NULL, names(res), selected='10km²'),
    selectInput('col', NULL, names(res[[1]])[-c(1,2,length(res[[1]]))], selected='t_peas')
  ),
  absolutePanel(bottom=0, left=0,
    textOutput('text')
  )
)}


server <- function(input, output, session) {
  output$map <- renderLeaflet({
    if (is.character(res[[input$res]])) {
      res[[input$res]] = readRDS(res[[input$res]])
    }
    df = res[[input$res]]
    col = df[[input$col]]
    g = leaflet(df, options=leafletOptions(minZoom=6.5)) %>%
      addTiles(
        paste0('https://api.os.uk/maps/raster/v1/zxy/Light_3857/{z}/{x}/{y}.png?key=', key),
        attribution = paste('Contains OS data &copy; Crown copyright and database rights', format(Sys.Date(), '%Y'))
      )
    if (input$res %in% c('100km²', '10km²')) {
      g %>%
        addPolygons(
          stroke = FALSE,
          fillColor = ~pal(col),
          fillOpacity = 1,
          # popup = ~paste0(
          #   '<table>',
          #   '<tr><th style="float:right">Easting:&nbsp;&nbsp;</th><th>', x, '</th></tr>',
          #   '<tr><th style="float:right">Northing:&nbsp;&nbsp;</th><th>', y, '</th></tr>',
          #   '<tr><th style="float:right">Resolution:&nbsp;&nbsp;</th><th>', input$res, '</th></tr>',
          #   '<tr><th style="float:right">', input$col, ':&nbsp;&nbsp;</th><th>', col, '</th></tr>',
          #   '</table>'
          # )
          popup = popupTable(df)
        ) %>%
        addLegend('bottomright', pal=pal0, values=~col, title=NULL)
    } else if (input$res %in% c('1km²', 'ha', 'parcel')) {
      g %>%
        addHeatmap(~lng, ~lat, col, blur=40)
    }
  })
  output$text = renderText({
    m = input$map_shape_mouseover
    req(m)
    paste0(round(m$lng,3), ',', round(m$lat,3))
  })
}


shinyApp(ui, server)
