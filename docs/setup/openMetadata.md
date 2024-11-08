# Setting up OpenMetadata

## Logging into the OpenMetadata UI
To access the OpenMetadata UI, go to [http://localhost:8585](http://localhost:8585) and log in using credentials defined in [the initial setup step](/setup/getting_setup).

* **Email:** This will consist of two environment variables from `.env.om_server` in the form below. The default email is `admin@open-metadata.org`.

       `AUTHORIZER_ADMIN_PRINCIPALS@AUTHORIZER_PRINCIPAL_DOMAIN`

* **Password:** the default password will be `admin`. You should probably [change that password](#changing-your-password) on your first login.

<center>![](/assets/imgs/openmetadata/om_setup__initial_login.png)</center>

!!! note

    If you're hosting the ADWH system on another machine, replace `localhost` with the domain name or IP addess of that remote machine.


## Define a Connection to the Data Warehouse Database

1. Go to **Settings** > **Services** > **Databases** and **Add a New Service**

    <center>![](/assets/imgs/openmetadata/om_setup__make_conn_1.png)</center>
    <center>![](/assets/imgs/openmetadata/om_setup__make_conn_2.png)</center>
    <center>![](/assets/imgs/openmetadata/om_setup__make_conn_3.png)</center>
    <center>![](/assets/imgs/openmetadata/om_setup__make_conn_4.png)</center>

2. Make a Postgres Database Service

    <center>![](/assets/imgs/openmetadata/om_setup__make_conn_5.png)</center>

    2.a. Enter a name and a brief description of the database.

    <center>![](/assets/imgs/openmetadata/om_setup__make_conn_6.png)</center>

    2.b. Enter credentials for the DWH database role you want the service to use along with the other connection info. After entering credentials and other info, **Test your Connection**.

    <center>![](/assets/imgs/openmetadata/om_setup__make_conn_7.png)</center>
    <center>![](/assets/imgs/openmetadata/om_setup__make_conn_8.png)</center>

    If all connection checks pass, click **OK** and **Save** the connection.

If everything was successful, you should now see your `ADWH Data Warehouse` **Database Service**.

<center>![](/assets/imgs/openmetadata/om_setup__make_conn_9.png)</center>

## Configure Metadata Ingestion

Contuing from the last image, go to the page for the `ADWH Data Warehouse` **Database Service**. You will see the different postgres databases in the `dwh_db` postgres instance.

To scan those databases for **Data Assets** to catalog, you have to configure an **Ingestion**.

1. Click **Ingestions** > **Add Ingestion** > **Add Metadata Ingestion**

    <center>![](/assets/imgs/openmetadata/om_setup__make_ingestion_1.png)</center>

    <center>![](/assets/imgs/openmetadata/om_setup__make_ingestion_2.png)</center>

2. Specify the assets to include in this **Metadata Ingestion** configuration

    Enter regex patterns to specify which database(s), schema(s), and table(s) should be included.

    <center>![](/assets/imgs/openmetadata/om_setup__make_ingestion_3.png)</center>

    <center>![](/assets/imgs/openmetadata/om_setup__make_ingestion_4.png)</center>

3. Set the schedule for running this **Ingestion** then **Add & Deploy** it

    <center>![](/assets/imgs/openmetadata/om_setup__make_ingestion_5.png)</center>
    <center>![](/assets/imgs/openmetadata/om_setup__make_ingestion_6.png)</center>

4. Trigger an initial **Run** to immediately run the **Metadata Ingestion**

    <center>![](/assets/imgs/openmetadata/om_setup__make_ingestion_7.png)</center>

Now you should have a catalog of metadata for all **Data Assets** in the main DWH schemas.

### Changing your password

1. From the user profile dropdown in the upper right corner, click your username to addess your user page

    ![](/assets/imgs/openmetadata/om_setup__pw_change_1.png)

2. Change your password

    ![](/assets/imgs/openmetadata/om_setup__pw_change_2.png)

    <center>![](/assets/imgs/openmetadata/om_setup__pw_change_3.png)</center>