import download
import clean
import processing
import overall_processing


def run_app():
    """
    Main function that calls and runs all scripts. Please view terminal for output on progress of downloads
    """
    OVERALL = False

    # Download
    download.download_all()

    # Clean up
    clean_up()

    # Processing
    process(OVERALL)

    # Visualization
    # Run notebooks


def clean_up():
    """
    Function to call all clean up values
    """
    # Create clean up class
    data_clean = clean.cleanup()

    # Clean up different datasets
    data_clean.yellow_cleanup()
    data_clean.green_cleanup()
    data_clean.fhvhv_cleanup()
    data_clean.citibike_cleanup()

    # Delete class and stop spark session
    del data_clean

def process(overall: bool):
    """
    Function to process the data and call all processing methods
    """
    if overall:
        proc = overall_processing.Processing()
        proc.process()
        del proc
    else:
        proc = processing.Processing()
        proc.process()
        del proc
    
    

run_app()