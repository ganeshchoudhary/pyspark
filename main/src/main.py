# This is a sample Python script.


# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

def print_hi(con=None):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {con.postgress_table}')  # Press Ctrl+F8 to toggle the breakpoint.


class Config:
    pass


if __name__ == '__main__':
    IsLiveCustomer_dict = {
        '1.0': 'Client On boarded or Proposal is under process',
        '2.0': 'Disburement Done',
        '3.0': 'Loan Account Closed',
        '6.0': 'Loan Account/Proposal Cancelled'
    }
    print(IsLiveCustomer_dict.items())
