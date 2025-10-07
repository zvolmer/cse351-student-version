"""
Course    : CSE 351
Assignment: 02
Student   : <Zac Volmer>

Instructions:
    - review instructions in the course
"""

# Don't import any other packages for this assignment
import os
import random
import threading
from money import *
from cse351 import *

# ---------------------------------------------------------------------------
def main():

    print('\nATM Processing Program:')
    print('=======================\n')

    create_data_files_if_needed()

    # Load ATM data files
    data_files = get_filenames('data_files')
    print('Found data files:', data_files)
    print('Number of files:', len(data_files))

    log = Log(show_terminal=True)
    log.start_timer()

    bank = Bank()

    readers = []
    for filename in data_files:
        print(f'starting thread for {filename}')
        reader = ATM_Reader(filename, bank)
        reader.start()
        readers.append(reader)

    for r in readers:
        r.join()

    test_balances(bank)

    log.stop_timer('Total time')



# ===========================================================================
class ATM_Reader(threading.Thread):

    def __init__(self, filename, bank):
        super().__init__()
        self.filename = filename
        self.bank = bank

    def run(self):
        try:
            with open(self.filename, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                    parts = line.split(',')
                    if len(parts) < 3:
                        continue
                    try:
                        account_number = int(parts[0])
                        trans_type = parts[1].strip().lower()
                        amount_str = parts[2].strip()
                    except Exception:
                        continue

                    if trans_type == 'd':
                        self.bank.deposit(account_number, amount_str)
                    elif trans_type == 'w':
                        self.bank.withdraw(account_number, amount_str)
                    else:
                        continue
        except FileNotFoundError:
            return


# ===========================================================================
class Account:

    def __init__(self, account_number):
        self.account_number = account_number
        self._balance = Money('0.00')
        self._lock = threading.Lock()

    def deposit(self, amount_str):
        m = Money(amount_str)
        with self._lock:
            self._balance.add(m)

    def withdraw(self, amount_str):
        m = Money(amount_str)
        with self._lock:
            self._balance.sub(m)

    def get_balance(self):
        with self._lock:
            digits = self._balance.digits  
            sign = ''
            num = digits
            if num and num[0] in ('+', '-'):
                sign = num[0]
                num = num[1:]

            if len(num) <= 2:
                num = num.zfill(3)

            dollars = num[:-2] if len(num) > 2 else '0'
            cents = num[-2:]
            money_str = f'{sign}{dollars}.{cents}'
            return Money(money_str)

# ===========================================================================
class Bank:

    def __init__(self):
        self._accounts = {}
        self._accounts_lock = threading.Lock()

    def _get_or_create_account(self, account_number):
        acct = self._accounts.get(account_number)
        if acct is not None:
            return acct

        with self._accounts_lock:
            acct = self._accounts.get(account_number)
            if acct is None:
                acct = Account(account_number)
                self._accounts[account_number] = acct
            return acct

    def deposit(self, account_number, amount_str):
        acct = self._get_or_create_account(account_number)
        acct.deposit(amount_str)

    def withdraw(self, account_number, amount_str):
        acct = self._get_or_create_account(account_number)
        acct.withdraw(amount_str)

    def get_balance(self, account_number):
        acct = self._accounts.get(account_number)
        if acct is None:
            return Money('0.00')
        return acct.get_balance()


# ---------------------------------------------------------------------------

def get_filenames(folder):
    """ Don't Change """
    filenames = []
    for filename in os.listdir(folder):
        if filename.endswith(".dat"):
            filenames.append(os.path.join(folder, filename))
    return filenames

# ---------------------------------------------------------------------------
def create_data_files_if_needed():
    """ Don't Change """
    ATMS = 10
    ACCOUNTS = 20
    TRANSACTIONS = 250000

    sub_dir = 'data_files'
    if os.path.exists(sub_dir):
        return

    print('Creating Data Files: (Only runs once)')
    os.makedirs(sub_dir)

    random.seed(102030)
    mean = 100.00
    std_dev = 50.00

    for atm in range(1, ATMS + 1):
        filename = f'{sub_dir}/atm-{atm:02d}.dat'
        print(f'- {filename}')
        with open(filename, 'w') as f:
            f.write(f'# Atm transactions from machine {atm:02d}\n')
            f.write('# format: account number, type, amount\n')

            # create random transactions
            for i in range(TRANSACTIONS):
                account = random.randint(1, ACCOUNTS)
                trans_type = 'd' if random.randint(0, 1) == 0 else 'w'
                amount = f'{(random.gauss(mean, std_dev)):0.2f}'
                f.write(f'{account},{trans_type},{amount}\n')

    print()

# ---------------------------------------------------------------------------
def test_balances(bank):
    """ Don't Change """

    # Verify balances for each account
    correct_results = (
        (1, '59362.93'),
        (2, '11988.60'),
        (3, '35982.34'),
        (4, '-22474.29'),
        (5, '11998.99'),
        (6, '-42110.72'),
        (7, '-3038.78'),
        (8, '18118.83'),
        (9, '35529.50'),
        (10, '2722.01'),
        (11, '11194.88'),
        (12, '-37512.97'),
        (13, '-21252.47'),
        (14, '41287.06'),
        (15, '7766.52'),
        (16, '-26820.11'),
        (17, '15792.78'),
        (18, '-12626.83'),
        (19, '-59303.54'),
        (20, '-47460.38'),
    )

    wrong = False
    for account_number, balance in correct_results:
        bal = bank.get_balance(account_number)
        print(f'{account_number:02d}: balance = {bal}')
        if Money(balance) != bal:
            wrong = True
            print(f'Wrong Balance: account = {account_number}, expected = {balance}, actual = {bal}')

    if not wrong:
        print('\nAll account balances are correct')



if __name__ == "__main__":
    main()