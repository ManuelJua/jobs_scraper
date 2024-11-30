import os
import re
import streamlit as st
import pandas as pd
import numpy as np
import folium
from streamlit_folium import folium_static
from folium.plugins import MarkerCluster, FastMarkerCluster
import plotly.express as px
from sqlalchemy import create_engine,text
from dotenv import load_dotenv

# load_dotenv()


def filter_df(keywords: str, df: pd.DataFrame):
    if keywords:
        # Compile the regex pattern beforehand
        full_pattern = ''.join([f'(?=.*{keyword})' for keyword in keywords.split()])
        pattern = re.compile(full_pattern, flags=re.IGNORECASE)
        
        # Apply the pattern to the dataframe
        contains_pattern = df['description'].apply(pattern.match).notna()
    else:
        contains_pattern = pd.Series([True] * len(df), index=df.index)

    # Filter based on other criteria
    not_na_lat = df['latitude'].notna()
    mean_salary_gt_5000 = df['salary'] > 5000
    
    # Combine using `loc` for efficiency
    filtered_df = df.loc[contains_pattern & not_na_lat & mean_salary_gt_5000]
    
    return filtered_df


def create_folium_map(df):
    # Initialize a Folium map centered on the mean latitude and longitude
    m = folium.Map(location=[df['latitude'].mean(
    ), df['longitude'].mean()], zoom_start=4
    )
   
    # Create lists for locations and popups
    df['popups'] = df.apply(lambda row: f"<a href='{row['job_url']}' target='_blank'>{row['job_title']}</a>", axis=1)
    faster_marker_data = df[['latitude', 'longitude','popups']].values.tolist()
    

    # Create a FasterMarkerCluster
    marker_cluster = FastMarkerCluster(data=faster_marker_data,callback="""
        function (row) {
            var marker = L.marker(new L.LatLng(row[0], row[1]));
            var popup = row[2];
            marker.bindPopup(popup);
            return marker;
        }
        """,lazy=True).add_to(m)

    folium.LayerControl().add_to(m)


    return m

def create_barplot(df):
        
    # Create a bar plot
    bar_fig = px.histogram(df, x='salary', title='Mean Salary Distribution')
    xmax = df['salary'].mean() + 4 * df['salary'].std()
    bar_fig.update_layout(xaxis=dict(range=[0, xmax]))
    
    # Calculate the median salary
    median_salary = int(df['salary'].median())

    
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
    conection_string=os.getenv('DATABASE_URL')
    engine=create_engine(conection_string)
    with engine.connect() as conn:
        result=conn.execute(text(""" SELECT * FROM jobs"""))
        df=pd.DataFrame(result)
    
    
    return df

def main():
    st.set_page_config(layout="wide")

    #Load data
    df = load_data()

    # Title of the app
    st.title("Reed Jobs Dashboard")

    # Keywords input
    keywords = st.text_input("Enter keywords (separated by spaces):")


    filtered_df=filter_df(keywords=keywords,df=df)

    # First row with map and barplot
    st.subheader("Map and Bar Plot",)
    col1, col2 = st.columns(spec=[0.5,0.5],gap='small',vertical_alignment='center')


    with col1:
        fig = create_barplot(df=filtered_df)
        st.plotly_chart(fig)

    with col2:
        m=create_folium_map(df=filtered_df)
        folium_static(fig=m,width=500,height=390)

    # Second row with dataframe
    st.subheader("Dataframe")
    st.dataframe(filtered_df)

if __name__=='__main__':
    main()
