import pandas as pd
import investpy
from datetime import datetime

# ==========================================================================================================================
'''
Author: Yasser Bennani
Title: FTSE 100 Data Scraper Part 2 (2014-2021)
'''
# ==========================================================================================================================


def months_between_date(start_date, end_date):
    '''Calculate number of months between two dates
    (Make sure to give end dates in format Year-Month-31/30 not Year-Month+1-01)
        args:
            start_date(str): string in format "yyyy-mm-dd" representing the starting date of the trading period desired
            end_date(str): string in format "yyyy-mm-dd" representing the ending date of the trading period desired
        returns:
            (int): number of months between start_date and end_date
    '''
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    start_date = datetime.strptime(start_date, "%Y-%m-%d")

    # with or without +1 ?
    return ((end_date.year - start_date.year) * 12 + (end_date.month - start_date.month)+1)


def common_constituents(constituents, start_date, end_date):
    '''
    Filter the constituents that are common during the specified trading period
        args:
            constituents(DataFrame): dataframe of index historical constituents
            start_date(str): string in format "yyyy-mm-dd" representing the starting date of the trading period desired
            end_date(str): string in format "yyyy-mm-dd" representing the ending date of the trading period desired
        returns:
            (DataFrame): symbols of the constituents that are common during the specified trading period between start_date and end_date
    '''
    # Get constituents of the index during the wanted period
    constituents = constituents.loc[start_date:end_date].dropna(
        axis=0, how='any')
    # Get stock symbols that are present in the index during the totality of the desired period
    f = months_between_date(start_date, end_date)
    freq = constituents[['Symbol']].value_counts()
    symbols = freq.loc[freq == f].reset_index()['Symbol']
    return symbols


def match_company_symbol(constituents, symbols):
    '''
    Match each company with its symbol
        args:
            constituents(DataFrame): dataframe of index historical constituents
            start_date(str): string in format "yyyy-mm-dd" representing the starting date of the trading period desired
        returns:
            (DataFrame): all input constituents matched with their symbols
    '''
    print("Matching constituents with their symbols...")
    constituents_w_symbols = pd.DataFrame(
        {'Date': [], 'Company': [], 'Symbol': []})
    for i in range(int(len(constituents)//100)):
        merged = pd.merge(
            constituents.iloc[i*100:(i+1)*100, :], symbols, on='Company')
        constituents_w_symbols = pd.concat([constituents_w_symbols, merged])

    print("Done matching constituents with their symbols.")
    return constituents_w_symbols


def scrape_constituents_symbols(constituents):
    '''
    This function uses the search api in investpy to scrape the constituents' symbols from investing.com
        args:
            constituents(DataFrame): dataframe of index historical constituents
        returns:
            (DataFrame): symbols of all the input constituents
            (DataFrame): companies whose symbols are not found on investing.com
    '''
    # all constituents to figure in the ftse 100 from 2014 to 2021
    constituents = constituents.drop_duplicates('Company')
    # initialize variables
    constituents_symbols = pd.DataFrame({'Company': [], 'Symbol': []})
    errors = pd.DataFrame({'Company': []})
    #
    print("Scraping the symbols of each constituent included in the index...")
    # loop through the list of constituents
    for i in range(len(constituents)):
        try:
            #print("Iteration :", i)
            search_result = investpy.search_quotes(text=constituents.iat[i, 1], products=['stocks'],
                                                   countries=['united kingdom'], n_results=1)
            #print("\n ", search_result)
            constituents_symbols = constituents_symbols.append(
                {'Company': constituents.iat[i, 1], 'Symbol': search_result.symbol}, ignore_index=True)
        except Exception as e:
            #print("Error: ", e)
            errors = errors.append(
                {'Company': constituents.iat[i, 1]}, ignore_index=True)
            constituents_symbols = constituents_symbols.append(
                {'Company': constituents.iat[i, 1], 'Symbol': ""}, ignore_index=True)
            continue
    print("Done sraping symbols.")

    return constituents_symbols, errors


def scrape_daily_returns(constituents, start_date, end_date):
    '''
    Scrape the daily percentage change returns of FTSE 100 constituents from start_date to end_date
        args:
            constituents(DataFrame): dataframe of index historical constituents between start_date and end_date along with their symbols
            start_date(str): string in format "yyyy-mm-dd" representing the starting date of the trading period desired
            end_date(str): string in format "yyyy-mm-dd" representing the ending date of the trading period desired
        returns:
            (DataFrame): daily percentage change returns of every constituent included in the index from start_date to end_date
    '''
    symbols = constituents[["Date", "Symbol"]].drop_duplicates("Symbol")
    #
    print("Scraping percentage change returns...")
    #
    search1 = investpy.search_quotes(text=symbols.iat[0, 1], products=['stocks'],
                                     countries=['united kingdom'], n_results=1)
    # print(search1)
    searched_data = search1.retrieve_historical_data(from_date=start_date, to_date=end_date).reset_index(
    )[['Date', 'Change Pct']].rename(columns={'Change Pct': symbols.iat[0, 1]}).iloc[::-1]
    # loop through constituents
    for i in range(1, len(symbols)):
        try:
            search_result = investpy.search_quotes(text=symbols.iat[i, 1], products=['stocks'],
                                                   countries=['united kingdom'], n_results=1)
            # print(search_result)
            returns = search_result.retrieve_historical_data(from_date=start_date, to_date=end_date).reset_index(
            )[['Date', 'Change Pct']].rename(columns={'Change Pct': symbols.iat[i, 1]}).iloc[::-1]

            searched_data = pd.merge(
                searched_data, returns, how='outer', on='Date')
        except Exception as e:
            #print("Error: ", e)
            continue
    print("Done scraping percentage change returns.")

    return searched_data


def main():
    print("Initiating...")
    # load reconstructed constituents scraped using r script
    constituents = pd.read_csv(
        "./historical_consts.csv")[['Date', 'Company']]
    # set up start and end dates of desired period
    start_date = '01/01/2014'
    end_date = '01/07/2021'
    # scrape symbols for all constituents
    symbols, _ = scrape_constituents_symbols(constituents)
    # match each constituent with their symbol
    comp_w_symb = match_company_symbol(constituents, symbols)
    # scrape daily percentage change returns of all constituents
    pc_returns = scrape_daily_returns(comp_w_symb, start_date, end_date)
    # save daily returns
    pc_returns.to_csv("pc_change_returns.csv")
    print("Done.")


if __name__ == "__main__":
    main()
