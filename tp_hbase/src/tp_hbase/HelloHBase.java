package tp_hbase;
/**
 * This brief HELLO WORLD Java program is meant to enable you to very quickly
 * gain a rudimentary, hands-on understanding of how data (and metadata) is 
 * stored and retrieved in HBase via the "client API".
 * 
 * PART 1: CONCEPTS
 * ================
 * For those coming to the HBase world with previous experience in traditional
 * RDBMS databases, it is essential to realize that Tables, Rows, and Columns 
 * in HBase, while bearing some resemblance to their namesakes in the RDBMS 
 * world, differ markedly in their structures and functionality.
 * 
 * **Column Families**
 * As you can see in the code below, when you use the Admin#createTable method,
 * besides providing a TableName, you also must specify at least one "Column 
 * Family" (denoted in the code by the class HColumnDescriptor).
 * In HBase, Columns are all grouped by Column Family, with all Columns in a
 * family being physically stored together. Theoretically, you could have a
 * large number of Column Families, but the present HBase architecture
 * actually has a practical limitation of no more than three or four per Table.
 * 
 * **Versioning**
 * In the code below, a "maxVersions" value of 3 is assigned to the 
 * Column Family, which means that versioning has been enabled for all Columns
 * in the family: when a Column is updated, the 2 most recent *previous* values
 * for that Column are still retrievable, each designated by a timestamp.
 * These individual versioned instances are sometimes referred to as the Cells
 * of a Column. The retrieval of multiple versions (Cells) of the same Column is 
 * performed below in the #getAndPrintAllCellVersions method.
 * 
 * **Columns**
 * It is important to note (in the most striking departure from RDBMS norms) 
 * that Columns themselves are NOT part of the Table definition. Columns are 
 * "defined" on-the-fly as each row is <put> (i.e., inserted/updated) into the 
 * database. There is also NO datatyping of each Column: HBase accepts any
 * byte-array of any length/format you wish to store in any Column. This means 
 * that IT IS UP TO THE INDIVIDUAL APPLICATION TO MANAGE AND ENFORCE COLUMN 
 * NAMES AND DATATYPES.  In the RDBMS world, the database (i.e., database 
 * administrator) manages column metadata; in the HBase world, the application 
 * (i.e., application designer/programmer) manages column metadata.
 * 
 * **Rows**
 * Rows are inserted, accessed, and physically ordered exclusively by Row ID 
 * (the conceptual equivalent of an RDBMS primary key). When a "scan" is
 * performed to access multiple contiguous rows, those rows will always be 
 * returned in Row ID order (either ascending or descending).
 * 
 * PART 2: WORKING WITH THIS CODE
 * ==============================
 * Importantly, your ability to run this code requires that you have successfully
 * installed and started a standalone implementation of HBase on the machine
 * on which this program is to be run. 
 * 
 * The recommended steps to take to run this program are:
 *   (1) Install a "standalone" configuration of the current stable release of
 *       HBase on your machine following the instructions provided at:
 *         https://hbase.apache.org/book.html#quickstart
 *       (If you are installing on a Windows machine, it is strongly recommended
 *       that you NOT bother trying to do an installation using the documented
 *       Cygwin option [which has proven to be faulty and is apparently not
 *       kept up-to-date with new releases of HBase], but instead install and 
 *       run a virtual Unix environment [e.g., Ubuntu] in a virtual machine
 *       such as VirtualBox, and install HBase in that environment.)
 *   (2) Copy this code into a new project in your favorite IDE, set up the 
 *       CLASSPATH as documented below, and use this code as your launchpad into
 *       effective utilization of the HBase Client API.  Run and modify this code 
 *       as extensively as you need to in order to build and deepen your 
 *       understanding of how to store and retrieve data (and metadata!) in HBase.
 *       Refer to the HBase javadocs ( https://hbase.apache.org/apidocs/ )
 *       to extend this code and explore functionality not demonstrated in the
 *       code below.
 *
 * This code was developed in coordination with HBase release 1.0.1.1; 
 * compatibility with subsequent releases is hoped for, but by no means 
 * guaranteed.
 * 
 * PART 3: CLASSPATH DETAILS
 * =========================
 * To fulfill CLASSPATH requirements to compile/run this program:
 *   -- the CLASSPATH must include the directory in which hbase-site.xml (i.e., 
 *      the HBase startup parameters file) is stored for your currently-running 
 *      instance of HBase (e.g., '/usr/local/hbase/hbase-1.0.1.1/conf').
 *      [In NetBeans, this would be set in Project Properties/Libraries/Run.]
 *   -- the CLASSPATH should also include the HBase library (e.g. "HBase_1.0.1.1"
 *      [In NetBeans, you can include this library in your project's 
 *      "Compile-time Libraries" list.]
 */
import java.io.IOException;
import java.util.Map.Entry;
import java.util.NavigableMap;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Successful running of this application requires access to an active instance
 * of HBase. For install instructions for a standalone instance of HBase, please
 * refer to https://hbase.apache.org/book.html#quickstart
 */
public final class HelloHBase {

  protected static final String MY_NAMESPACE_NAME = "myTestNamespace";
  static final TableName MY_TABLE_NAME = TableName.valueOf("myTestTable");
  static final byte[] MY_COLUMN_FAMILY_NAME = Bytes.toBytes("cf");
  static final byte[] MY_FIRST_COLUMN_QUALIFIER
          = Bytes.toBytes("myFirstColumn");
  static final byte[] MY_SECOND_COLUMN_QUALIFIER
          = Bytes.toBytes("mySecondColumn");
  static final byte[] MY_ROW_ID = Bytes.toBytes("rowId01");

  public static void main(final String[] args) throws IOException {
    final boolean deleteAllAtEOJ = true;

    /**
     * ConnectionFactory#createConnection() automatically looks for
     * hbase-site.xml (HBase configuration parameters) on the system's
     * CLASSPATH, to enable creation of Connection to HBase via Zookeeper.
     */
    try (Connection connection = ConnectionFactory.createConnection();
            Admin admin = connection.getAdmin()) {

      admin.getClusterStatus(); // assure connection successfully established
      System.out.println("\n*** Hello HBase! -- Connection has been "
              + "established via Zookeeper!!\n");

      createNamespaceAndTable(admin);

      System.out.println("Getting a Table object for [" + MY_TABLE_NAME
              + "] with which to perform CRUD operations in HBase.");
      try (Table table = connection.getTable(MY_TABLE_NAME)) {

        putRowToTable(table);
        getAndPrintRowContents(table);

        if (deleteAllAtEOJ) {
          deleteRow(table);
        }
      }

      if (deleteAllAtEOJ) {
        deleteNamespaceAndTable(admin);
      }
    }
  }

  /**
   * Invokes Admin#createNamespace and Admin#createTable to create a namespace
   * with a table that has one column-family.
   *
   * @param admin Standard Admin object
   * @throws IOException If IO problem encountered
   */
  static void createNamespaceAndTable(final Admin admin) throws IOException {

    if (!namespaceExists(admin, MY_NAMESPACE_NAME)) {
      System.out.println("Creating Namespace [" + MY_NAMESPACE_NAME + "].");

      admin.createNamespace(NamespaceDescriptor
              .create(MY_NAMESPACE_NAME).build());
    }
    if (!admin.tableExists(MY_TABLE_NAME)) {
      System.out.println("Creating Table [" + MY_TABLE_NAME.getNameAsString()
              + "], with one Column Family ["
              + Bytes.toString(MY_COLUMN_FAMILY_NAME) + "].");

      admin.createTable(new HTableDescriptor(MY_TABLE_NAME)
              .addFamily(new HColumnDescriptor(MY_COLUMN_FAMILY_NAME)));
    }
  }

  /**
   * Invokes Table#put to store a row (with two new columns created 'on the
   * fly') into the table.
   *
   * @param table Standard Table object (used for CRUD operations).
   * @throws IOException If IO problem encountered
   */
  static void putRowToTable(final Table table) throws IOException {

    table.put(new Put(MY_ROW_ID).addColumn(MY_COLUMN_FAMILY_NAME,
            MY_FIRST_COLUMN_QUALIFIER,
            Bytes.toBytes("Hello")).addColumn(MY_COLUMN_FAMILY_NAME,
                    MY_SECOND_COLUMN_QUALIFIER,
                    Bytes.toBytes("World!")));

    System.out.println("Row [" + Bytes.toString(MY_ROW_ID)
            + "] was put into Table ["
            + table.getName().getNameAsString() + "] in HBase;\n"
            + "  the row's two columns (created 'on the fly') are: ["
            + Bytes.toString(MY_COLUMN_FAMILY_NAME) + ":"
            + Bytes.toString(MY_FIRST_COLUMN_QUALIFIER)
            + "] and [" + Bytes.toString(MY_COLUMN_FAMILY_NAME) + ":"
            + Bytes.toString(MY_SECOND_COLUMN_QUALIFIER) + "]");
  }

  /**
   * Invokes Table#get and prints out the contents of the retrieved row.
   *
   * @param table Standard Table object
   * @throws IOException If IO problem encountered
   */
  static void getAndPrintRowContents(final Table table) throws IOException {

    Result row = table.get(new Get(MY_ROW_ID));

    System.out.println("Row [" + Bytes.toString(row.getRow())
            + "] was retrieved from Table ["
            + table.getName().getNameAsString()
            + "] in HBase, with the following content:");

    for (Entry<byte[], NavigableMap<byte[], byte[]>> colFamilyEntry
            : row.getNoVersionMap().entrySet()) {
      String columnFamilyName = Bytes.toString(colFamilyEntry.getKey());

      System.out.println("  Columns in Column Family [" + columnFamilyName
              + "]:");

      for (Entry<byte[], byte[]> columnNameAndValueMap
              : colFamilyEntry.getValue().entrySet()) {

        System.out.println("    Value of Column [" + columnFamilyName + ":"
                + Bytes.toString(columnNameAndValueMap.getKey()) + "] == "
                + Bytes.toString(columnNameAndValueMap.getValue()));
      }
    }
  }

  /**
   * Checks to see whether a namespace exists.
   *
   * @param admin Standard Admin object
   * @param namespaceName Name of namespace
   * @return true If namespace exists
   * @throws IOException If IO problem encountered
   */
  static boolean namespaceExists(final Admin admin, final String namespaceName)
          throws IOException {
    try {
      admin.getNamespaceDescriptor(namespaceName);
    } catch (NamespaceNotFoundException e) {
      return false;
    }
    return true;
  }

  /**
   * Invokes Table#delete to delete test data (i.e. the row)
   *
   * @param table Standard Table object
   * @throws IOException If IO problem is encountered
   */
  static void deleteRow(final Table table) throws IOException {
    System.out.println("Deleting row [" + Bytes.toString(MY_ROW_ID)
            + "] from Table ["
            + table.getName().getNameAsString() + "].");
    table.delete(new Delete(MY_ROW_ID));
  }

  /**
   * Invokes Admin#disableTable, Admin#deleteTable, and Admin#deleteNamespace to
   * disable/delete Table and delete Namespace.
   *
   * @param admin Standard Admin object
   * @throws IOException If IO problem is encountered
   */
  static void deleteNamespaceAndTable(final Admin admin) throws IOException {
    if (admin.tableExists(MY_TABLE_NAME)) {
      System.out.println("Disabling/deleting Table ["
              + MY_TABLE_NAME.getNameAsString() + "].");
      admin.disableTable(MY_TABLE_NAME); // Disable a table before deleting it.
      admin.deleteTable(MY_TABLE_NAME);
    }
    if (namespaceExists(admin, MY_NAMESPACE_NAME)) {
      System.out.println("Deleting Namespace [" + MY_NAMESPACE_NAME + "].");
      admin.deleteNamespace(MY_NAMESPACE_NAME);
    }
  }
}