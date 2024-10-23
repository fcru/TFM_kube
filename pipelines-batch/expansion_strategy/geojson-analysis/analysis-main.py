from top_stations import *
from mongo_potential_stations import *
from set_threshold import *

if __name__ == "__main__":
    print("Calculting Top Stations...")
    find_top_stations()
    print("Calculated..")

    print("Calculting Similar Stations...")
    get_similar_stations()
    print("Similar stations calculated")

    print("Setting Threshold...")
    # Example usage with default parameters (original Barcelona case)
    set_threshold()


    set_threshold(
        container_collection='bcn_neighbourhood',
        point_collection='educative_centers',
        geometry_field='geometry',
        name_field='name',
        classification_field='school_density'
    )

    set_threshold(
        container_collection='bcn_neighbourhood',
        point_collection='cultural_points_of_interests',
        geometry_field='geometry',
        name_field='name',
        classification_field='poi_density'
    )

    print("All set!")