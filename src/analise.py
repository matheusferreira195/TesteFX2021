import plotly.graph_objects as go # or plotly.express as px
from etl import main as run_etl
from config import Config

config = Config()
data = run_etl().toPandas()

fig = go.Figure(data=[go.Sankey(
    node = dict(
      pad = 15,
      thickness = 20,
      line = dict(color = "black", width = 0.5),
      label = list(config.TAG_PASSOS.keys()),
      color = "blue"
    ),
    link = dict(
      source = data['passo_anterior'].tolist(), 
      target = data['passo_atual'].tolist(),
      value = data['count'].tolist()
  ))])

fig.update_layout(title_text="Fluxos de Navegação", font_size=10)

import dash
import dash_core_components as dcc
import dash_html_components as html



app = dash.Dash()
app.layout = html.Div([
    dcc.Graph(figure=fig)
])

if __name__ == "__main__":
  app.run_server(debug=True, use_reloader=False, host='0.0.0.0')  