import pandas as pd
import os

def split_csv_by_branch(input_path, output_dir, chunksize=1000000):
    """
    Read the large CSV in chunks and write rows to separate CSVs by Brocker_id.
    """
    input_path = os.path.expanduser(input_path)
    output_dir = os.path.expanduser(output_dir)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Dictionary to track if we've already written headers to broker-specific files
    brokers_written = {}

    #files_list
    files_list = []
    
    # Read the large CSV in chunks
    for chunk in pd.read_csv(input_path, chunksize=chunksize, 
                            thousands=',',
                            dtype={"Ticker":"string",
                                    "Name":"string",
                                    "buy":int,
                                    "sell":int,
                                    "diff":int,
                                    "Branch":"string",
                                    "Date":"string",
                                    "Branch_Code":"string"}):
        
        # Group by Brocker_id in this chunk
        groups = chunk.groupby('Branch_Code')
        
        for Branch_Code, gdf in groups:
            # Determine output file name for this broker
            broker_file = os.path.join(output_dir, f"{Branch_Code}.csv")
            
            # If first time writing this file, include headers
            if Branch_Code not in brokers_written:
                gdf.to_csv(broker_file, index=False, mode='w')
                brokers_written[Branch_Code] = True
                files_list.append(Branch_Code)
            else:
                # Append to existing file, no header
                gdf.to_csv(broker_file, index=False, mode='a', header=False)

    print("Splitting completed. Each Branch CSV is in:", output_dir)
    return files_list

if __name__ == '__main__':
    input_csv = '~/Stock_project/TW_stock_data/broker_trading_list.csv'
    output_dir = '~/Stock_project/TW_stock_data/small_broker_trading'
    files_list_csv = '~/Stock_project/TW_stock_data/small_broker_trading_list.csv'
    files_list = split_csv_by_branch(input_csv, output_dir)
    df = pd.DataFrame(files_list)
    df.to_csv(files_list_csv, index=False, mode='w')
