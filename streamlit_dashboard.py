import streamlit as st
import pandas as pd
import numpy as np
import folium
from streamlit_folium import folium_static
from folium.plugins import MarkerCluster, FastMarkerCluster
import pydeck as pdk
import plotly.express as px

def filter_df(keywords:str,df:pd.DataFrame):
    if keywords:
        # Filter the dataframe based on keywords
        patterns = [f'(?=.*{keyword})' for keyword in keywords.split()]
        full_pattern = ''.join(patterns)
        filtered_df = df[(df['jobDescription'].str.contains(pat=full_pattern,regex=True, case=False)) & (df['lat'].notna()) & (df['meanSalary']>5000)]
    else:
        filtered_df = df[(df['lat'].notna()) & (df['meanSalary']>5000)]

    return filtered_df

def df_display(df):
    return df[['jobTitle','locationName','meanSalary','date','employerName','expirationDate','jobUrl','applications']]

def create_folium_map(df):
    # Initialize a Folium map centered on the mean latitude and longitude
    m = folium.Map(location=[df['lat'].mean(
    ), df['lon'].mean()], zoom_start=4
    )
   
    # Create lists for locations and popups
    df['popups'] = df.apply(lambda row: f"<a href='{row['jobUrl']}' target='_blank'>{row['jobTitle']}</a>", axis=1)
    faster_marker_data = df[['lat', 'lon','popups']].values.tolist()
    

    # Create a MarkerCluster
    marker_cluster = FastMarkerCluster(data=faster_marker_data,callback="""
        function (row) {
            var marker = L.marker(new L.LatLng(row[0], row[1]));
            var popup = row[2];
            marker.bindPopup(popup);
            return marker;
        }
        """).add_to(m)

    # marker_cluster.add_to(m)

    folium.LayerControl().add_to(m)


    return m

def create_barplot(df):
        
    # Create a bar plot
    bar_fig = px.histogram(df, x='meanSalary', title='Mean Salary Distribution')
    xmax = df['meanSalary'].mean() + 4 * df['meanSalary'].std()
    bar_fig.update_layout(xaxis=dict(range=[0, xmax]))
    
    # Calculate the median salary
    median_salary = int(df['meanSalary'].median())

    
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
        showlegend=False,
        height=400,
        width=500
    )
    


    return bar_fig


@st.cache_data(persist="disk")
def load_data():
    # Load and process your data here
    df = pd.read_parquet('software_developer_reed_jobs.parquet').pipe(
        lambda df: df[['jobId', 'employerName', 'jobTitle', 'locationName', 'minimumSalary',
                       'maximumSalary', 'currency', 'expirationDate', 'date', 'jobDescription',
                       'applications', 'jobUrl','lat','lon']])
    
    # Calculate mean salary
    df['meanSalary'] = df[['minimumSalary', 'maximumSalary']].mean(axis=1)
    
    return df



st.set_page_config(layout="wide")

#Load data
df = load_data()

# Title of the app
st.title("Reed Jobs Dashboard")

# Keywords input
keywords = st.text_input("Enter keywords (separated by spaces):")


filtered_df=filter_df(keywords=keywords,df=df)

# First row with map and barplot
st.subheader("Map and Bar Plot")
col1, col2 = st.columns(spec=[0.5,0.5],gap='small',vertical_alignment='center')


with col1:
    fig = create_barplot(df=filtered_df)
    st.plotly_chart(fig)

with col2:
    m=create_folium_map(df=filtered_df)
    folium_static(fig=m,width=500,height=390)

# Second row with dataframe
st.subheader("Dataframe")
st.dataframe(df_display(filtered_df))
