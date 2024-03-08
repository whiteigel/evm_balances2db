import sqlalchemy
import psycopg2
from prettytable import PrettyTable
import web3
from web3 import Web3
from config import token, rpc, db
from data import wallets

def main(selected_network, selected_token):
    try:
        # Установка соединения с базой данных
        engine = sqlalchemy.create_engine(db)
        connection = engine.connect()

        # Ethereum connection
        def get_data(selected_network, selected_token):
            connect = Web3(web3.HTTPProvider(rpc[selected_network]))
            with open('abi_token.json', 'r') as file:
                abi = file.read()
            contract = connect.eth.contract(address=token[selected_network][selected_token], abi=abi)
            decimals = contract.functions.decimals().call()

            data = []
            for profile, wallet in wallets.items():
                tx = connect.eth.get_transaction_count(wallet, "latest")
                balance_eth = round(connect.eth.get_balance(wallet) / (10 ** 18), 5)
                balance_selected_token = round(contract.functions.balanceOf(wallet).call() / (10 ** decimals), 3)
                data.append((profile, wallet, tx, balance_eth, balance_selected_token))
            return data

        data = get_data(selected_network, selected_token)

        # Insert data into database
        table_name = selected_network + "_stats"
        for item in data:
            existing_record = connection.execute(
                sqlalchemy.text(f"SELECT 1 FROM {table_name} WHERE address = :address"),
                {"address": item[1]}
            ).fetchone()

            if existing_record:
                # Update the existing record
                connection.execute(
                    sqlalchemy.text(
                        f"UPDATE {table_name} SET tx = :tx, scroll_native = :balance_eth, {selected_token} = :balance_selected_token, date = CURRENT_DATE WHERE address = :address"
                    ),
                    {"address": item[1], "tx": item[2], "balance_eth": item[3], "balance_selected_token": item[4]}
                )
            else:
                # Insert a new record
                connection.execute(
                    sqlalchemy.text(
                        f"INSERT INTO {table_name} (profile_id, address, tx, scroll_native, {selected_token}, date) VALUES (:profile, :wallet, :tx, :balance_eth, :balance_selected_token, CURRENT_DATE)"
                    ),
                    {"profile": item[0], "wallet": item[1], "tx": item[2], "balance_eth": item[3], "balance_selected_token": item[4]}
                )

        # Fetch and print the required columns from the scroll_stats or zksync_stats table
        sql_query = sqlalchemy.text(f"SELECT profile_id, address, tx, scroll_native, {selected_token}, date FROM {table_name}")
        result = connection.execute(sql_query)
        rows = result.fetchall()

        # Print the data in a table
        table = PrettyTable(['Profile ID', 'Wallet Address', 'Transactions', 'Native Balance', selected_token.upper(), 'Date'])
        for row in rows:
            table.add_row(row)
        print(table)

        # Commit the transactions
        connection.commit()

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Close the database connection
        connection.close()

# Пример использования функции main
if __name__ == "__main__":
    selected_network = "scroll"  # Изменить на "zksync", если хотите использовать другую сеть
    selected_token = "usdc"  # Изменить на "usdt", если хотите получить баланс другого токена
    main(selected_network, selected_token)
