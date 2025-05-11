from smartabasepy.user_ import get_user, edit_user
from smartabasepy import UserOption, UserFilter
from pandas import DataFrame
import pandas as pd




hws_athletes = pd.read_csv("/Users/byach/Downloads/hws_athletes.csv")


# Women's volleyball

hws_wvb = get_user(
    url = "https://hws.smartabase.com/sportsmed",
    username = "amsconsultant",
    password = "Rk68n2NR5SD4",
    filter = UserFilter(
        user_key="group",
        user_value = "Women's Volleyball"
    ),
    option= UserOption(
        cache=False
    )
)


hws_wvb_joined = hws_wvb.merge(hws_athletes, 
                how = "left",
                left_on="email",
                right_on="Email Alt")

hws_wvb_import = hws_wvb_joined[[
    "user_id",
    "about",
    "first_name",
    "last_name",
    "dob",
    "username",
    "sex",
    "Email",
    "ORG ID"
]]

hws_wvb_import = hws_wvb_import.rename(
    columns = {
        "Email":"email",
        "ORG ID":"uuid",
    }
)


hws_wvb_results_df = edit_user(
    users_df=hws_wvb_import,
    url = "https://hws.smartabase.com/sportsmed",
    username = "amsconsultant",
    password = "Rk68n2NR5SD4",
    option=UserOption(interactive_mode=True)
)


# All

hws_all = get_user(
    url = "https://hws.smartabase.com/sportsmed",
    username = "amsconsultant",
    password = "Rk68n2NR5SD4",
    filter = UserFilter(
        user_key="group",
        user_value = "ALL ATHLETES"
    ),
    option= UserOption(
        cache=False
    )
)

hws_full = hws_all.merge(hws_athletes, 
                how = "inner",
                left_on="email",
                right_on="Email Alt")

hws_full_import = hws_full[[
    "user_id",
    "about",
    "first_name",
    "last_name",
    "dob",
    "username",
    "sex",
    "Email",
    "ORG ID"
]]

hws_full_import = hws_full_import.rename(
    columns = {
        "Email":"email",
        "ORG ID":"uuid",
    }
)


hws_full_results_df = edit_user(
    users_df=hws_full_import,
    url = "https://hws.smartabase.com/sportsmed",
    username = "amsconsultant",
    password = "Rk68n2NR5SD4",
    option=UserOption(interactive_mode=True)
)

# UT Martin ----

from pandas import DataFrame
import pandas as pd
from internal_scripts.update_date_of_birth import update_user_date_of_birth
from smartabasepy.user_option import UserOption


utm_sa = pd.read_csv("/Users/byach/Downloads/utm_sa.csv")

utm_dob = pd.read_csv("/Users/byach/Downloads/UTM_DOB.csv")

# Creating about

utm_sa['about'] = utm_sa['First Name'] + " " + utm_sa['Last Name']

utm_sa['about'] = utm_sa['about'].str.title()

utm_dob['about'] = utm_dob['First Name'] + " " + utm_dob['Last Name']

utm_dob['about'] = utm_dob['about'].str.title()

utm_dob = utm_dob.drop(['First Name', 'Last Name'], axis = 1)

# Convert string column to datetime, specifying US format (MM/DD/YY)
utm_dob['DOB'] = pd.to_datetime(utm_dob['DOB'], format='%m/%d/%y')

# Format as desired string output (%d/%m/%Y)
utm_dob['DOB'] = utm_dob['DOB'].dt.strftime('%d/%m/%Y')

utm_dob = utm_dob.rename(columns= {'DOB':'date_of_birth'})



# Update date of birth
failed_updates = update_user_date_of_birth(
    mapping_df=utm_dob,
    user_key="about",
    url="https://utm.smartabase.com/skyhawks",
    username="amsbuilder",
    password="HMGVb9C6FBxERcd9",
    option=UserOption(interactive_mode=True)
)

failed_updates.to_csv("/Users/byach/Downloads/utm_failed_updates.csv")


# ---- Barker College

from smartabasepy import edit_user, UserOption
import pandas as pd

bark_ath = pd.read_csv("/Users/byach/Downloads/barker_athletes.csv")

bark_ath = bark_ath.drop(['ID', 'First Name', 'Last Name', 'Username', 'Date Of Birth', 'Sex', 'Old - UUID'], axis = 1)

bark_ath = bark_ath.rename(columns={'New UUID': 'uuid',
                                    'Email Address': 'email'}
                           )


failed_df = edit_user(
    mapping_df = bark_ath,
    user_key = 'email',
    url = 'https://barker.smartabase.com/barker',
    username = 'byach',
    password = 'Brandon123!',
    option=UserOption(interactive_mode=True)
)
