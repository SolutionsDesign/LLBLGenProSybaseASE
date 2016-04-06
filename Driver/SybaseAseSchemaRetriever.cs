//////////////////////////////////////////////////////////////////////
// Part of the LLBLGen Pro Driver for Sybase ASE, used in the generated code. 
// LLBLGen Pro is (c) 2002-2016 Solutions Design. All rights reserved.
// http://www.llblgen.com
//////////////////////////////////////////////////////////////////////
// This Driver's sourcecode is released under the following license:
// --------------------------------------------------------------------------------------------
// 
// The MIT License(MIT)
//   
// Copyright (c)2002-2016 Solutions Design. All rights reserved.
// http://www.llblgen.com
//   
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//   
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//   
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//////////////////////////////////////////////////////////////////////
// Contributers to the code:
//		- Frans Bouma [FB]
//////////////////////////////////////////////////////////////////////
using System;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using SD.Tools.BCLExtensions.DataRelated;
using SD.Tools.BCLExtensions.CollectionsRelated;

using SD.LLBLGen.Pro.DBDriverCore;
using System.Data.Common;

namespace SD.LLBLGen.Pro.DBDrivers.SybaseAse
{
    /// <summary>
	/// SybaseAse specific implementation of DBSchemaRetriever
	/// </summary>
	public class SybaseAseSchemaRetriever : DBSchemaRetriever
	{
		/// <summary>
		/// CTor
		/// </summary>
		/// <param name="catalogRetriever">The catalog retriever.</param>
		public SybaseAseSchemaRetriever(SybaseAseCatalogRetriever catalogRetriever) : base(catalogRetriever)
		{
		}


		/// <summary>
		/// Retrieves the table- and field meta data for the tables which names are in the passed in elementNames and which are in the schema specified.
		/// </summary>
		/// <param name="schemaToFill">The schema to fill.</param>
		/// <param name="elementNames">The element names.</param>
		/// <remarks>Implementers should add DBTable instances with the DBTableField instances to the DBSchema instance specified.
		/// Default implementation is a no-op</remarks>
		protected override void RetrieveTableAndFieldMetaData(DBSchema schemaToFill, IEnumerable<DBElementName> elementNames)
		{
			DbConnection connection = this.DriverToUse.CreateConnection();

			DbCommand fieldCommand = this.DriverToUse.CreateStoredProcCallCommand(connection, "sp_columns");
			DbParameter tableNameParameter = this.DriverToUse.CreateParameter(fieldCommand, "@table_name", string.Empty);
			this.DriverToUse.CreateParameter(fieldCommand, "@table_owner", schemaToFill.SchemaOwner);
			DbDataAdapter fieldAdapter = this.DriverToUse.CreateDataAdapter(fieldCommand);

			DbCommand pkCommand = this.DriverToUse.CreateStoredProcCallCommand(connection, "sp_pkeys");
			DbParameter pkRetrievalTableNameParameter = this.DriverToUse.CreateParameter(pkCommand, "@table_name", string.Empty);
			this.DriverToUse.CreateParameter(pkCommand, "@table_owner", schemaToFill.SchemaOwner);
			DbDataAdapter pkRetrievalAdapter = this.DriverToUse.CreateDataAdapter(pkCommand);

			DbCommand ucCommand = this.DriverToUse.CreateStoredProcCallCommand(connection, "sp_statistics");
			DbParameter ucRetrievalTableNameParameter = this.DriverToUse.CreateParameter(ucCommand, "@table_name", string.Empty);
			this.DriverToUse.CreateParameter(ucCommand, "@table_owner", schemaToFill.SchemaOwner);
			this.DriverToUse.CreateParameter(ucCommand, "@is_unique", "Y");
			DbDataAdapter ucRetrievalAdapter = this.DriverToUse.CreateDataAdapter(ucCommand);

			DataTable ucFieldsInTable = new DataTable();
			DataTable fieldsInTable = new DataTable();
			DataTable pkFieldsInTable = new DataTable();

			try
			{
				connection.Open();
				fieldCommand.Prepare();
				pkCommand.Prepare();
				ucCommand.Prepare();

				// walk all tables in this schema and pull information for this table out of the db.
				List<DBTable> tablesToRemove = new List<DBTable>();
				foreach(DBElementName tableName in elementNames)
				{
					DBTable currentTable = new DBTable(schemaToFill, tableName);
					schemaToFill.Tables.Add(currentTable);

					tableNameParameter.Value = currentTable.Name;

					fieldsInTable.Clear();
					fieldAdapter.Fill(fieldsInTable);

					try
					{
						int ordinalPosition = 1;
						var fields = from row in fieldsInTable.AsEnumerable()
									 let datatypeFragments = row.Value<string>("type_name").Split(' ')
									 let isIdentity = ((datatypeFragments.Length > 1) && (datatypeFragments[1].ToLowerInvariant() == "identity")) 
									 let typeDefinition = CreateTypeDefinition(row, datatypeFragments, isIdentity)
									 select new DBTableField(row.Value<string>("column_name"), typeDefinition, row.Value<string>("remarks") ?? string.Empty)
									 {
										OrdinalPosition = ordinalPosition++,
										IsIdentity = isIdentity,
										IsNullable = (row.Value<int>("nullable")==1),
										ParentTable = currentTable
									 };
						currentTable.Fields.AddRange(fields);

						// get Primary Key fields for this table
						pkRetrievalTableNameParameter.Value = currentTable.Name;
						pkFieldsInTable.Clear();
						pkRetrievalAdapter.Fill(pkFieldsInTable);

						foreach(DataRow row in pkFieldsInTable.AsEnumerable())
						{
							string columnName = row.Value<string>("column_name");
							DBTableField primaryKeyField = currentTable.FindFieldByName(columnName);
							if(primaryKeyField != null)
							{
								primaryKeyField.IsPrimaryKey = true;
								// PrimaryKeyConstraintName is not set, because the name of the primary key constraint is unfortunately not obtainable from the meta-data.
							}
						}

						// get UC fields for this table
						ucRetrievalTableNameParameter.Value = currentTable.Name;
						ucFieldsInTable.Clear();
						ucRetrievalAdapter.Fill(ucFieldsInTable);
						// only rows of type '3' are interesting
						var ucFieldsPerUc = from row in ucFieldsInTable.AsEnumerable()
											where row.Value<int>("type") == 3
											group row by row.Value<string>("index_name") into g
											select g;

						foreach(IGrouping<string, DataRow> ucFields in ucFieldsPerUc)
						{
							DBUniqueConstraint currentUC = new DBUniqueConstraint(ucFields.Key) { AppliesToTable = currentTable };
							bool addUc = true;
							foreach(DataRow row in ucFields)
							{
								DBTableField currentField = currentTable.FindFieldByName(row.Value<string>("column_name"));
								if(currentField == null)
								{
									continue;
								}
								currentUC.Fields.Add(currentField);
							}
							addUc &= (currentUC.Fields.Count > 0);
							if(addUc)
							{
								currentTable.UniqueConstraints.Add(currentUC);
								currentUC.AppliesToTable = currentTable;
							}
						}
					}
					catch(ApplicationException ex)
					{
						// non fatal error, remove the table, proceed 
						schemaToFill.LogError(ex, "Table '" + currentTable.Name + "' removed from list due to an internal exception in Field population: " + ex.Message, "SybaseAseSchemaRetriever::RetrieveTableAndFieldMetaData");
						tablesToRemove.Add(currentTable);
					}
					catch(InvalidCastException ex)
					{
						// non fatal error, remove the table, proceed 
						schemaToFill.LogError(ex, "Table '" + currentTable.Name + "' removed from list due to cast exception in Field population.", "SybaseAseSchemaRetriever::RetrieveTableAndFieldMetaData");
						tablesToRemove.Add(currentTable);
					}
				}

				foreach(DBTable toRemove in tablesToRemove)
				{
					schemaToFill.Tables.Remove(toRemove);
				}
			}
			finally
			{
				connection.SafeClose(true);
			}
		}


		/// <summary>
		/// Retrieves the view- and field meta data for the views which names are in the passed in elementNames and which are in the schema specified.
		/// </summary>
		/// <param name="schemaToFill">The schema to fill.</param>
		/// <param name="elementNames">The element names.</param>
		/// <remarks>Implementers should add DBView instances with the DBViewField instances to the DBSchema instance specified.
		/// Default implementation is a no-op</remarks>
		protected override void RetrieveViewAndFieldMetaData(DBSchema schemaToFill, IEnumerable<DBElementName> elementNames)
		{
			DbConnection connection = this.DriverToUse.CreateConnection();

			DbCommand fieldCommand = this.DriverToUse.CreateStoredProcCallCommand(connection, "sp_columns");
			DbParameter tableNameParameter = this.DriverToUse.CreateParameter(fieldCommand, "@table_name", string.Empty);
			this.DriverToUse.CreateParameter(fieldCommand, "@table_owner", schemaToFill.SchemaOwner);
			DbDataAdapter fieldAdapter = this.DriverToUse.CreateDataAdapter(fieldCommand);
			DataTable fieldsInView = new DataTable();

			List<DBView> viewsToRemove = new List<DBView>();
			try
			{
				connection.Open();
				fieldCommand.Prepare();

				foreach(DBElementName viewName in elementNames)
				{
					DBView currentView = new DBView(schemaToFill, viewName);
					schemaToFill.Views.Add(currentView);

					tableNameParameter.Value = currentView.Name;

					// get the fields. 
					fieldsInView.Clear();
					fieldAdapter.Fill(fieldsInView);

					try
					{
						int ordinalPosition = 1;
						var fields = from row in fieldsInView.AsEnumerable()
									 let datatypeFragments = row.Value<string>("type_name").Split(' ')
									 let typeDefinition = CreateTypeDefinition(row, datatypeFragments, false)
									 select new DBViewField(row.Value<string>("column_name"), typeDefinition, row.Value<string>("remarks") ?? string.Empty)
									 {
										 OrdinalPosition = ordinalPosition++,
										 IsNullable = (row.Value<int>("nullable") == 1),
										 ParentView = currentView
									 };
						currentView.Fields.AddRange(fields);
					}
					catch(ApplicationException ex)
					{
						// non fatal error, remove the view, proceed 
						schemaToFill.LogError(ex, "View '" + currentView.Name + "' removed from list due to an internal exception in Field population: " + ex.Message, "SybaseAseSchemaRetriever::RetrieveViewAndFieldMetaData");
						viewsToRemove.Add(currentView);
					}
					catch(InvalidCastException ex)
					{
						// non fatal error, remove the view, proceed 
						schemaToFill.LogError(ex, "View '" + currentView.Name + "' removed from list due to cast exception in Field population.", "SybaseAseSchemaRetriever::RetrieveViewAndFieldMetaData");
						viewsToRemove.Add(currentView);
					}
				}
			}
			finally
			{
				connection.SafeClose(true);
			}
			foreach(DBView toRemove in viewsToRemove)
			{
				schemaToFill.Views.Remove(toRemove);
			}
		}


		/// <summary>
		/// Retrieves the stored procedure- and parameter meta data for the stored procedures which names are in the passed in elementNames and which are
		/// in the schema specified.
		/// </summary>
		/// <param name="schemaToFill">The schema to fill.</param>
		/// <param name="elementNames">The element names.</param>
		/// <remarks>Implementers should add DBStoredProcedure instances with the DBStoredProcedureParameter instances to the DBSchema instance specified.
		/// Default implementation is a no-op</remarks>
		protected override void RetrieveStoredProcedureAndParameterMetaData(DBSchema schemaToFill, IEnumerable<DBElementName> elementNames)
		{
			DbConnection connection = this.DriverToUse.CreateConnection();

			DbCommand parameterCommand = this.DriverToUse.CreateStoredProcCallCommand(connection, "sp_sproc_columns");
			DbParameter procNameParameter = this.DriverToUse.CreateParameter(parameterCommand, "@procedure_name", string.Empty);
			this.DriverToUse.CreateParameter(parameterCommand, "@procedure_owner", schemaToFill.SchemaOwner);
			DbDataAdapter parameterAdapter = this.DriverToUse.CreateDataAdapter(parameterCommand);
			DataTable parameterRows = new DataTable();
			List<DBStoredProcedure> storedProceduresToRemove = new List<DBStoredProcedure>();
			try
			{
				connection.Open();
				parameterCommand.Prepare();
				List<string> storedProceduresAdded = new List<string>();
				foreach(DBElementName procName in elementNames)
				{
					DBStoredProcedure storedProcedure = new DBStoredProcedure(schemaToFill, procName);
					schemaToFill.StoredProcedures.Add(storedProcedure);
					procNameParameter.Value = storedProcedure.Name.Replace("_", "[_]");
					parameterRows.Clear();
					parameterAdapter.Fill(parameterRows);
					storedProceduresAdded.Add(storedProcedure.Name);

					try
					{
						var parameters = from row in parameterRows.AsEnumerable()
										 let parameterName = row.Value<string>("column_name")
										 where parameterName.ToLowerInvariant() != "return_value"
										 let typeDefinition = CreateTypeDefinition(row, row.Value<string>("type_name").Split(' '), false)
										 select new DBStoredProcedureParameter(parameterName, typeDefinition, row.Value<string>("remarks") ?? string.Empty)
										 {
											 OrdinalPosition = row.Value<int>("ordinal_position"),
											 Direction = DetermineParameterDirection(row.Value<string>("mode")),
											 ParentStoredProcedure = storedProcedure
										 };
						storedProcedure.Parameters.AddRange(p => p.OrdinalPosition, parameters);
					}
					catch(InvalidCastException ex)
					{
						// non fatal error, remove the stored procedure, proceed 
						schemaToFill.LogError(ex, "Stored procedure '" + storedProcedure.Name + "' removed from list due to cast exception in Parameter population.", "SybaseAseSchemaRetriever::RetrieveStoredProcedureAndParameterMetaData");
						storedProceduresToRemove.Add(storedProcedure);
					}
					catch(ApplicationException ex)
					{
						// non fatal error, remove the table, proceed 
						schemaToFill.LogError(ex, "Stored procedure '" + storedProcedure.Name + "' removed from list due to an internal exception in Field population: " + ex.Message, "SybaseAseSchemaRetriever::RetrieveStoredProcedureAndParameterMetaData");
						storedProceduresToRemove.Add(storedProcedure);
					}
				}
				// now add the procs which names are in the list of elementNames which aren't added yet, as these procs don't have parameters. 
				var procsWithoutParameters = elementNames.Except(storedProceduresAdded.Select(s => new DBElementName(s)));
				foreach(DBElementName procName in procsWithoutParameters)
				{
					schemaToFill.StoredProcedures.Add(new DBStoredProcedure(schemaToFill, procName));
				}
			}
			finally
			{
				connection.SafeClose(true);
			}
			foreach(DBStoredProcedure toRemove in storedProceduresToRemove)
			{
				schemaToFill.StoredProcedures.Remove(toRemove);
			}
		}


		/// <summary>
		/// Determines the parameter direction.
		/// </summary>
		/// <param name="directionValue">The direction value.</param>
		/// <returns></returns>
		private static ParameterDirection DetermineParameterDirection(string directionValue)
		{
			ParameterDirection toReturn;
			switch(directionValue)
			{
				case "out":
					toReturn = ParameterDirection.Output;
					break;
				default:
					toReturn = ParameterDirection.Input;
					break;
			}
			return toReturn;
		}


		/// <summary>
		/// creates a new type definition from the data in the row specified.
		/// </summary>
		/// <param name="row">The row.</param>
		/// <param name="datatypeFragments">The datatype fragments.</param>
		/// <param name="isIdentity">if set to true, the element for which this type is for is an identity field, which is important for type discovery</param>
		/// <returns>a filled DBTypeDefinition instance</returns>
		private DBTypeDefinition CreateTypeDefinition(DataRow row, string[] datatypeFragments, bool isIdentity)
		{
			if(isIdentity)
			{
				// all typenames are called 'numeric identity', so we've to obtain the type name from a dictionary
				string typename = "int";
				switch(row.Value<int>("data_type"))
				{
					case -6:
						typename = "tinyint";
						break;
					case -5:
						typename = "bigint";
						break;
					case 2:
						typename = "numeric";
						break;
					case 3:
						typename = "decimal";
						break;
					case 4:
						typename = "int";
						break;
					case 5:
						typename = "smallint";
						break;
					case 6:
						typename = "float";
						break;
					case 7:
						typename = "real";
						break;
				}
				datatypeFragments[0] = typename;
			}
			int lengthInBytes = row.Value<int>("length");
			int dbType = SybaseAseDBDriver.ConvertStringToDBType(datatypeFragments[0], lengthInBytes);
			int precision = 0;
			int length = 0;
			if(this.DriverToUse.DBTypeIsNumeric(dbType))
			{
				precision = (row.Value<int?>("precision") ?? 0); 
			}
			else
			{
				length = (row.Value<int?>("char_octet_length") ?? 0);
			}
			int scale = row.Value<int?>("scale") ?? 0;

			DBTypeDefinition toReturn = new DBTypeDefinition();
			toReturn.SetDBType(dbType, this.DriverToUse, length, precision, scale);
			return toReturn;
		}
	}
}
