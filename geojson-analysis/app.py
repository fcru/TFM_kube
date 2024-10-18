import streamlit as st
import folium
from folium.plugins import MarkerCluster
from streamlit_folium import folium_static
import pandas as pd
from utilities import doc_to_gdf, filter_estacio_to_gdf

def get_potential_stations_summary(potential_stations, neighborhood_geometry=None):
    """Calculate summary statistics for potential stations, optionally filtered by neighborhood"""
    if potential_stations is None or potential_stations.empty:
        return None

    # Filter out stations with cluster = -1
    valid_stations = potential_stations[potential_stations['cluster'] != -1]

    # If neighborhood geometry is provided, filter stations within that neighborhood
    if neighborhood_geometry is not None:
        valid_stations = valid_stations[valid_stations.geometry.within(neighborhood_geometry)]

    if valid_stations.empty:
        return None

    # Calculate averages for distance metrics
    distance_metrics = {
        'Education Distance': 'dist_education',
        'Shopping Distance': 'dist_shopping',
        'POI Distance': 'dist_cpoi',
        'Bus Stops Distance': 'dist_bus_stops',
        'Metro Stations Distance': 'dist_metro_stations',
        'Bike Lanes Distance': 'dist_bike_lanes',
        'Popular Places Distance': 'dist_popular',
        'Bike Station Distance': 'dist_bike_station'
    }

    summary = {
        'Total Proposed Stations': len(valid_stations),
        'Average Distances': {}
    }

    for label, col in distance_metrics.items():
        if col in valid_stations.columns:
            summary['Average Distances'][label] = valid_stations[col].mean()

    return summary


def get_color_by_density(density):
    """Return color based on density classification"""
    colors = {
        'very_high': '#9A051F',
        'high': '#ff0000',
        'medium': '#ffaa00',
        'low': '#ffff00'
    }
    return colors.get(density, '#808080')


def prepare_geojson_data(gdf, density_fields):
    """Prepare GeoDataFrame for GeoJSON conversion by flattening nested structures"""
    gdf = gdf.copy()

    for field in density_fields:
        class_col = f"{field}_class"
        count_col = f"{field}_count"

        gdf[class_col] = gdf[field].apply(
            lambda x: x['classification'] if isinstance(x, dict) and 'classification' in x else 'unknown'
        )
        gdf[count_col] = gdf[field].apply(
            lambda x: x['count'] if isinstance(x, dict) and 'count' in x else 0
        )

    return gdf

def style_function(feature, active_field):
    """Style function for GeoJSON layers"""
    class_col = f"{active_field}_class"
    density = feature['properties'].get(class_col, 'unknown')
    return {
        'fillColor': get_color_by_density(density),
        'color': 'black',
        'weight': 1,
        'fillOpacity': 0.6
    }


def filter_neighborhoods_by_density(gdf, density_thresholds):
    """Filter neighborhoods based on density thresholds"""
    mask = pd.Series(True, index=gdf.index)

    density_levels = {
        'low': 1,
        'medium': 2,
        'high': 3,
        'very_high': 4
    }

    for field, threshold in density_thresholds.items():
        class_col = f"{field}_class"
        if threshold > 0:  # Only apply filter if threshold is set
            field_mask = gdf[class_col].map(density_levels) >= threshold
            mask &= field_mask

    return gdf[mask]


def create_map(gdf_stations, gdf_no_bikes, gdf_no_docks, gdf_high, potential_stations,
               gdf_density, visible_layers, density_thresholds):
    """Create Folium map with selectable layers"""
    m = folium.Map(location=[41.3851, 2.1734], zoom_start=12)

    # Prepare density data
    density_fields = ['commercial_density', 'school_density', 'poi_density']
    gdf_density = prepare_geojson_data(gdf_density, density_fields)

    # Filter neighborhoods based on density thresholds
    if any(threshold > 0 for threshold in density_thresholds.values()):
        gdf_density = filter_neighborhoods_by_density(gdf_density, density_thresholds)

    # Add density layers
    if 'density_layers' in visible_layers:
        for field in density_fields:
            display_name = field.replace('_density', '').replace('_', ' ').title()
            class_col = f"{field}_class"

            if class_col in gdf_density.columns:
                folium.GeoJson(
                    gdf_density,
                    name=display_name,
                    style_function=lambda x, field=field: style_function(x, field),
                    tooltip=folium.GeoJsonTooltip(
                        fields=['name'] + [f"{f}_class" for f in density_fields] + [f"{f}_count" for f in
                                                                                    density_fields],
                        aliases=['Neighborhood:'] +
                                [f'{f.replace("_density", "").replace("_", " ").title()} Level:' for f in
                                 density_fields] +
                                [f'{f.replace("_density", "").replace("_", " ").title()} Count:' for f in
                                 density_fields],
                        labels=True
                    ),
                    show=(field in visible_layers)
                ).add_to(m)

    # Add station markers based on visibility settings
    marker_configs = [
        (gdf_no_bikes, 'red', 'No Bikes', 'no_bikes'),
        (gdf_no_docks, 'orange', 'No Dockers', 'no_docks'),
        (gdf_high, 'pink', 'High Rotations', 'high_rotation'),
        (potential_stations[potential_stations['cluster'] != -1] if potential_stations is not None else None,
         'green', 'Potential Station', 'potential')
    ]

    for gdf, color, station_type, layer_name in marker_configs:
        if gdf is not None and layer_name in visible_layers:
            for _, row in gdf.iterrows():
                popup_text = (f"Station: {row.get('name', 'N/A')} "
                              f"<br>Id: {row.get('station_id', 'N/A')} "
                              f"<br>Type: {station_type}")

                folium.Marker(
                    location=[row.geometry.y, row.geometry.x],
                    popup=popup_text,
                    icon=folium.Icon(color=color, icon='info-sign')
                ).add_to(m)

    # Add regular stations in a cluster if selected
    if gdf_stations is not None and 'regular_stations' in visible_layers:
        mc = MarkerCluster()
        for _, row in gdf_stations.iterrows():
            folium.Marker(
                location=[row.geometry.y, row.geometry.x],
                popup=f"Station: {row['name']} <br>Id: {row.get('station_id', 'N/A')}",
                icon=folium.Icon(color='blue', icon='info-sign')
            ).add_to(mc)
        mc.add_to(m)

    # Add layer control and legend
    folium.LayerControl().add_to(m)
    add_legend(m)

    return m


def add_legend(m):
    """Add a comprehensive legend to the map"""
    legend_html = '''
    <div style="position: fixed; 
                bottom: 50px; left: 50px; width: 200px;
                border:2px solid grey; z-index:9999; font-size:14px;
                background-color:white;
                padding: 10px;
                border-radius: 5px;
                opacity: 0.9;
                ">
        <div style="margin-bottom: 10px;">
            <strong>Density Levels</strong>
            <div style="display: flex; flex-direction: column; gap: 5px; margin-top: 5px;">
                <div style="display: flex; align-items: center;">
                    <div style="background-color: #9A051F; width: 20px; height: 20px; margin-right: 5px;"></div>
                    <span>Very High</span>
                </div>
                <div style="display: flex; align-items: center;">
                    <div style="background-color: #ff0000; width: 20px; height: 20px; margin-right: 5px;"></div>
                    <span>High</span>
                </div>
                <div style="display: flex; align-items: center;">
                    <div style="background-color: #ffaa00; width: 20px; height: 20px; margin-right: 5px;"></div>
                    <span>Medium</span>
                </div>
                <div style="display: flex; align-items: center;">
                    <div style="background-color: #ffff00; width: 20px; height: 20px; margin-right: 5px;"></div>
                    <span>Low</span>
                </div>
            </div>
        </div>
        <hr>
        <div style="margin-top: 10px;">
            <strong>Station Types</strong>
            <div style="display: flex; flex-direction: column; gap: 5px; margin-top: 5px;">
                <div><span style="color: red;">●</span> No Bikes</div>
                <div><span style="color: orange;">●</span> No Docks</div>
                <div><span style="color: pink;">●</span> High Rotation</div>
                <div><span style="color: green;">●</span> Potential</div>
                <div><span style="color: blue;">●</span> Regular</div>
            </div>
        </div>
    </div>
    '''
    m.get_root().html.add_child(folium.Element(legend_html))

def get_safe_columns(df, desired_columns):
    """Return only the columns that exist in the DataFrame"""
    return [col for col in desired_columns if col in df.columns]


def display_potential_stations_table(potential_stations):
    """Display potential stations table with available columns"""
    if potential_stations is not None and not potential_stations.empty:
        st.header("Potential Stations")

        # Define desired columns and get those that are available
        desired_columns = ['name', 'station_id', 'geometry']
        available_columns = get_safe_columns(potential_stations, desired_columns)

        if not available_columns:
            st.warning("No displayable columns found in potential stations data")
            return

        # Create a display DataFrame without geometry column
        display_columns = [col for col in available_columns if col != 'geometry']
        if display_columns:
            # Convert geometry to string representation if needed
            display_df = potential_stations[display_columns].copy()
            st.dataframe(display_df)

            # Display number of potential stations
            st.write(f"Total potential stations: {len(display_df)}")
        else:
            st.warning("No displayable columns available in potential stations data")
    else:
        st.warning("No potential stations data available")


def display_neighborhood_data(gdf_density, neighborhood_name, all_stations):
    """Display detailed information for selected neighborhood"""
    if neighborhood_name:
        st.subheader(f"Data for {neighborhood_name}")

        try:
            # Get neighborhood data
            neighborhood = gdf_density[gdf_density['name'] == neighborhood_name].iloc[0]

            # Display density information
            density_cols = ['commercial_density', 'school_density', 'poi_density']
            density_data = []
            for col in density_cols:
                if col in neighborhood:
                    density_info = neighborhood[col]
                    density_data.append({
                        'Type': col.replace('_density', '').title(),
                        'Level': density_info['classification'],
                        'Count': density_info['count']
                    })

            if density_data:
                st.write("Density Information:")
                st.table(pd.DataFrame(density_data))
            else:
                st.write("No density information available for this neighborhood")

            # Find stations in the neighborhood
            neighborhood_geometry = neighborhood.geometry
            stations_in_area = all_stations[all_stations.geometry.within(neighborhood_geometry)]

            if not stations_in_area.empty:
                st.write("Stations in this neighborhood:")
                st.table(stations_in_area[['name', 'station_id']])
            else:
                st.write("No stations found in this neighborhood")

        except IndexError:
            st.error(f"Neighborhood '{neighborhood_name}' not found in the dataset")
        except Exception as e:
            st.error(f"An error occurred while processing the data: {str(e)}")

def display_summary_metrics(summary, title):
    """Display summary metrics in a consistent format"""
    if summary:
        st.subheader(title)
        st.write(f"Total Proposed Stations: {summary['Total Proposed Stations']}")

        st.write("Average Distances:")
        metrics_df = pd.DataFrame(
            list(summary['Average Distances'].items()),
            columns=['Metric', 'Value']
        )
        metrics_df['Value'] = metrics_df['Value'].round(2)
        st.dataframe(metrics_df, hide_index=True)
    else:
        st.warning(f"No data available for {title}")


def create_layout():
    """Create the main layout with two columns"""
    st.title("Barcelona Bicing Stations Interactive Dashboard")

    # Create two columns
    col1, col2 = st.columns([2, 1])

    return col1, col2


def main():
    st.title("Barcelona Bicing Stations Interactive Dashboard")

    # Initialize session state for storing filter settings
    if 'visible_layers' not in st.session_state:
        st.session_state.visible_layers = []
    if 'density_thresholds' not in st.session_state:
        st.session_state.density_thresholds = {}
    if 'apply_clicked' not in st.session_state:
        st.session_state.apply_clicked = False

    try:
        # Load all required data
        gdf_top_stations, top_stations_error = doc_to_gdf('top_stations',
                                                          fields=['name', 'station_id', 'geometry', 'reason'])
        if top_stations_error:
            st.error(f"Error loading top stations: {top_stations_error}")
            return

        # Filter stations by reason
        gdf_no_bikes = gdf_top_stations[
            gdf_top_stations['reason'].apply(lambda x: 'No Bikes' in x)] if gdf_top_stations is not None else None
        gdf_no_docks = gdf_top_stations[
            gdf_top_stations['reason'].apply(lambda x: 'No Docks' in x)] if gdf_top_stations is not None else None
        gdf_high = gdf_top_stations[
            gdf_top_stations['reason'].apply(lambda x: 'High Rotation' in x)] if gdf_top_stations is not None else None

        potential_stations, potential_error = doc_to_gdf('proposed_station',
                                                         fields=['name', 'station_id', 'geometry', 'dist_education',
                                                                 'dist_shopping', 'dist_cpoi', 'dist_bus_stops',
                                                                 'dist_metro_stations', 'dist_bike_lanes',
                                                                 'dist_popular', 'dist_bike_station',
                                                                 'similarity_score', 'cluster'])

        if potential_error:
            st.error(f"Error loading potential stations: {potential_error}")

        gdf_stations, stations_error = filter_estacio_to_gdf(gdf_top_stations)
        st.write("ok filter")
        if stations_error:
            st.error(f"Error loading stations: {stations_error}")

        gdf_density, density_error = doc_to_gdf('bcn_neighbourhood',
                                                fields=['name', 'geometry', 'commercial_density',
                                                        'school_density', 'poi_density'])
        if density_error:
            st.error(f"Error loading density data: {density_error}")
            return
        # Create main tabs
        map_tab, stats_tab = st.tabs(["Map View", "Statistics"])
        # Map Tab

        with map_tab:
            # Move the sidebar content here, above the map
            with st.expander ("Map Filters"):
                with st.form("filter_form"):
                    st.header("Map Controls")
                    temp_visible_layers = []
                    # Density layer controls (simplified)
                    col1, col2 = st.columns(2)

                    with col1:
                        st.subheader("Density Layers")
                        temp_visible_layers.append('density_layers')
                        density_options = {
                            'commercial_density': "Commercial",
                            'school_density': "Schools",
                            'poi_density': "Points of Interest"
                        }

                        for key, label in density_options.items():
                            if st.checkbox(label, True):
                                temp_visible_layers.append(key)
                    with col2:
                        # Station visibility controls
                        st.subheader("Station Types")
                        station_types = {
                            'regular_stations': "Regular Stations",
                            'no_bikes': "No Bikes Issues",
                            'no_docks': "No Docks Issues",
                            'high_rotation': "High Rotation",
                            'potential': "Potential Stations"
                        }
                        for key, label in station_types.items():
                            if st.checkbox(label, True):
                                temp_visible_layers.append(key)

                    # Density filters
                    st.subheader("Density Filters")
                    temp_density_thresholds = {}
                    density_levels = ['None', 'Low', 'Medium', 'High', 'Very High']

                    for density_type in density_options.keys():
                        display_name = density_options[density_type]
                        selected_level = st.select_slider(
                            f"Minimum {display_name} Level",
                            options=range(len(density_levels)),
                            value=0,
                            format_func=lambda x: density_levels[x]
                        )
                        temp_density_thresholds[density_type] = selected_level

                    if st.form_submit_button("Apply Filters"):
                        st.session_state.visible_layers = temp_visible_layers
                        st.session_state.density_thresholds = temp_density_thresholds
                        st.session_state.apply_clicked = True

            # Create and display map using the stored filter settings
            m = create_map(
                gdf_stations, gdf_no_bikes, gdf_no_docks, gdf_high,
                potential_stations, gdf_density,
                st.session_state.visible_layers,
                st.session_state.density_thresholds
            )
            folium_static(m)

        # Statistics Tab
        with stats_tab:
                col1, col2 = st.columns(2)

                with col1:
                    # Global Statistics
                    global_summary = get_potential_stations_summary(potential_stations)
                    display_summary_metrics(global_summary, "Global Statistics")

                with col2:
                    # Neighborhood Selection and Statistics
                    st.subheader("Neighborhood Information")
                    if gdf_density is not None and 'name' in gdf_density.columns:
                        selected_neighborhood = st.selectbox(
                            "Select a neighborhood:",
                            options=[''] + sorted(gdf_density['name'].unique().tolist())
                        )

                        if selected_neighborhood:
                            neighborhood = gdf_density[gdf_density['name'] == selected_neighborhood].iloc[0]

                            # Calculate and display neighborhood-specific statistics
                            neighborhood_summary = get_potential_stations_summary(potential_stations,
                                                                                  neighborhood.geometry)
                            display_summary_metrics(neighborhood_summary, f"Statistics for {selected_neighborhood}")

                            # Display density information
                            st.subheader("Density Information")
                            density_cols = ['commercial_density', 'school_density', 'poi_density']
                            density_data = []
                            for col in density_cols:
                                if col in neighborhood:
                                    density_info = neighborhood[col]
                                    density_data.append({
                                        'Type': col.replace('_density', '').title(),
                                        'Level': density_info['classification'],
                                        'Count': density_info['count']
                                    })

                            if density_data:
                                st.dataframe(pd.DataFrame(density_data), hide_index=True)
                    else:
                        st.warning("Neighborhood data is not available")

    except Exception as e:
        st.error(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    main()