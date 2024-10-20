import streamlit as st
import pandas as pd
import numpy as np
import folium
from streamlit_folium import folium_static
from folium.plugins import MarkerCluster
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

    # Initialize a MarkerCluster object
    marker_cluster = MarkerCluster().add_to(m)

    # Add markers to the cluster
    for _, row in df.iterrows():
        folium.Marker(
            location=[row['lat'], row['lon']],
            popup=f"<a href='{row['jobUrl']}' target='_blank'>{row['jobTitle']}</a>"
        ).add_to(marker_cluster)

    return m

def create_barplot(df):
        
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
        showlegend=False,
        height=400,
        width=500
    )
    


    return bar_fig


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
df=reed_df.rename(columns={'latitude':'lat','longitude':'lon'})

 
st.set_page_config(layout="wide")
# Title of the app
st.title("Reed Jobs Dashboard")

# Keywords input
keywords = st.text_input("Enter keywords (separated by spaces):")


filtered_df=filter_df(keywords=keywords,df=df)

# First row with map and barplot
st.subheader("Map and Bar Plot")
col1, col2 = st.columns(2)


with col1:
    fig = create_barplot(df=filtered_df)
    st.plotly_chart(fig)

with col2:
    m=create_folium_map(df=filtered_df)
    folium_static(fig=m,width=500,height=390)

# Second row with dataframe
st.subheader("Dataframe")
st.dataframe(df_display(filtered_df))

