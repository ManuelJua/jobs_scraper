import pandas as pd
import numpy as np
import plotly.express as px
from dash import dcc, html,Dash,dash_table
from dash.dependencies import Input, Output


# Load and remove duplicate job listings based on 'jobId' from both JSON files
reed_df = pd.read_json('software_developer_reed_jobs.json').drop_duplicates(subset=['jobId']).reset_index(drop=True).pipe(
    lambda df: df[['jobId', 'employerName', 'jobTitle', 'locationName', 'minimumSalary',
                   'maximumSalary', 'currency', 'expirationDate', 'date', 'jobDescription',
                   'applications', 'jobUrl']]
)
# Convert job titles and descriptions to lowercase
reed_df[['jobTitle', 'jobDescription']] = reed_df[[
    'jobTitle', 'jobDescription']].applymap(str.lower)

#Calculate mean salary
reed_df['meanSalary'] = reed_df.apply(
    lambda df: df[['minimumSalary', 'maximumSalary']].mean(), axis=1)


#Get coordinates
location_df = pd.read_csv('location.csv').drop_duplicates().reset_index()
reed_df = reed_df.merge(location_df[['locationName', 'latitude', 'longitude']], 
                          on='locationName', 
                          how='left')

def filter_df(df, keywords):
    filtered_df = df[(df['jobDescription'].str.contains(pat=keywords, regex=True, case=False)) & (df['meanSalary']>5000)]
   
    return filtered_df


# Initialize the Dash app
app = Dash(__name__)
server=app.server

# Layout of the app
app.layout = html.Div(children=[
    html.H1(children='Reed Jobs Dashboard'),
    dcc.Input(id='keyword-input', type='text', placeholder='Enter keyword', value=''),
    html.Div(id='keyword-output'),
    html.Div([
        dcc.Graph(id='bar-plot', style={'display': 'inline-block', 'width': '49%', 'height': '300px'}),
        dcc.Graph(id='map-plot', style={'display': 'inline-block', 'width': '49%', 'height': '300px'})
    ],style={'margin-bottom':'50px'}),
    html.Div(id="table-container")
])

# Callback to update the map plot based on keyword input
@app.callback(
    Output('map-plot', 'figure'),
    Input('keyword-input', 'value')
)
def update_mapplot(keyword):
    df=filter_df(reed_df,keyword)
    # Create a continuous color scale
    colorscale = px.colors.sequential.Viridis

    fig = px.scatter_map(data_frame=df, lat='latitude', lon='longitude',
                         hover_name='jobTitle', hover_data='locationName')
    fig.update_layout(
        map=dict(
            style="open-street-map",
            zoom=4,
            center=dict(lat=df['latitude'].mean(), lon=df['longitude'].mean())
        ),
        showlegend=False,
        # height=600,
        margin=dict(l=0, r=0, t=0, b=0)
    )

    # Define cluster steps and corresponding colors
    steps = [0, 10, 50, 100, 500, 1000, 5000, 10000]
    colors = [colorscale[int(i)] for i in np.linspace(
        0, len(colorscale)-1, len(steps))]

    # Enable clustering with color steps
    fig.update_traces(
        cluster=dict(
            enabled=True,
            step=steps,
            color=colors,
            opacity=0.7,
            size=list(range(10, 35, 5)),  # Increase size with cluster size
            maxzoom=15,
        )
    )

    return fig

# Callback to update the bar plot based on keyword input
@app.callback(
    Output('bar-plot', 'figure'),
    Output('keyword-output', 'children'),
    Input('keyword-input', 'value')
)
def update_barplot(keyword):
    # Filter the dataframe based on the keyword
    df = filter_df(reed_df, keyword)
    
    # Create a bar plot
    bar_fig = px.histogram(df, x='meanSalary', title='Mean Salary Distribution')
    xmax = reed_df['meanSalary'].mean() + 4 * reed_df['meanSalary'].std()
    bar_fig.update_layout(xaxis=dict(range=[0, xmax]))
    
    # Calculate the median salary
    median_salary = df['meanSalary'].median()
    
    # Add vertical line at the median salary
    bar_fig.add_shape(
        type='line',
        x0=median_salary, x1=median_salary, 
        y0=0, y1=1, xref='x', yref='paper',
        line=dict(color='red', dash='dash')
    )
    
    # Add text annotation for the median salary
    bar_fig.add_annotation(
        x=median_salary, y=1.05, xref='x', yref='paper',
        text=f'Median salary: {median_salary}',
        showarrow=False, font=dict(color='red')
    )
    
    # Update layout for bar plot
    bar_fig.update_layout(
        plot_bgcolor='white',
        paper_bgcolor='lightgrey',
        font=dict(family="Arial, sans-serif", size=12, color="black"),
        xaxis=dict(range=[0, xmax], title='Mean Salary', showgrid=True, gridwidth=1, gridcolor='LightPink'),
        yaxis=dict(title='Count', showgrid=True, gridwidth=1, gridcolor='LightPink'),
        margin=dict(l=40, r=40, t=40, b=40),
        showlegend=False
    )
    if keyword:
        results_string=f"Showing results for: '{keyword}'"
    else:
        results_string=f"Showing all jobs"


    return bar_fig, results_string

# Callback to update the table based on keyword input
@app.callback(
   Output('table-container', 'children'),
    Input('keyword-input', 'value')
)
def update_table(keyword):
    df = filter_df(reed_df, keyword)

    table = dash_table.DataTable(
        columns=[{"name": i, "id": i} for i in df.columns],
        data=df.to_dict('records'),
        fixed_rows={'headers': True},  # Fix the header
        style_table={'height': '300px','width':'95%', 'overflowY': 'auto','margin-left':'50px','margin-right':'50px'},  # Ensure the table scrolls and the header stays fixed
        style_cell={'overflow': 'hidden', 'textOverflow': 'ellipsis', 'maxWidth': 200},
        style_header={'backgroundColor': 'white', 'fontWeight':'bold'},
        style_cell_conditional=[
            {'if': {'column_id': 'jobDescription'}, 'maxWidth': '150px'},
            {'if': {'column_id': 'meanSalary'}, 'maxWidth': '150px'},
            {'if': {'column_id': 'latitude'}, 'maxWidth': '75px'},
            {'if': {'column_id': 'longitude'}, 'maxWidth': '75px'},
            {'if': {'column_id': 'jobTitle'}, 'maxWidth': '300px'},
            {'if': {'column_id': 'locationName'}, 'maxWidth': '100px'},
        ]
    )
    return table

if __name__ == '__main__':
    app.run_server(debug=False)