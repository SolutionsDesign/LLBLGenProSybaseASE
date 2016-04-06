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

using SD.LLBLGen.Pro.DBDriverCore;
using SD.Tools.BCLExtensions.DataRelated;
using System.Data.Common;

namespace SD.LLBLGen.Pro.DBDrivers.SybaseAse
{
    /// <summary>
    /// SybaseAse specific implementation of DBCatalogRetriever
    /// </summary>
	public class SybaseAseCatalogRetriever : DBCatalogRetriever
    {
        /// <summary>
        /// CTor
        /// </summary>
        public SybaseAseCatalogRetriever(SybaseAseDBDriver driverToUse) : base(driverToUse)
        {
        }


		/// <summary>
		/// Produces the DBSchemaRetriever instance to use for retrieving meta-data of a schema.
		/// </summary>
		/// <returns>ready to use schema retriever</returns>
		protected override DBSchemaRetriever CreateSchemaRetriever()
		{
			return new SybaseAseSchemaRetriever(this);
		}


		/// <summary>
		/// Retrieves all Foreign keys.
		/// </summary>
		/// <param name="catalogMetaData">The catalog meta data.</param>
		private void RetrieveForeignKeys(DBCatalog catalogMetaData)
		{
			if(catalogMetaData.Schemas.Count <= 0)
			{
				return;
			}

			#region Description of query used
			//select 
			//    sofk.name as FK_NAME,
			//	  sufk.name as FK_SCHEMA_NAME,
			//	  sofkt.name as FK_TABLE_NAME,
			//	  supk.name as PK_SCHEMA_NAME,
			//	  sopkt.name as PK_TABLE_NAME,
			//    sr.fokey1 as FK_ORDINAL1,
			//    sr.fokey2 as FK_ORDINAL2,
			//    sr.fokey3 as FK_ORDINAL3,
			//    sr.fokey4 as FK_ORDINAL4,
			//    sr.fokey5 as FK_ORDINAL5,
			//    sr.fokey6 as FK_ORDINAL6,
			//    sr.fokey7 as FK_ORDINAL7,
			//    sr.fokey8 as FK_ORDINAL8,
			//    sr.fokey9 as FK_ORDINAL9,
			//    sr.fokey10 as FK_ORDINAL10,
			//    sr.fokey11 as FK_ORDINAL11,
			//    sr.fokey12 as FK_ORDINAL12,
			//    sr.fokey13 as FK_ORDINAL13,
			//    sr.fokey14 as FK_ORDINAL14,
			//    sr.fokey15 as FK_ORDINAL15,
			//    sr.fokey16 as FK_ORDINAL16,
			//    sr.refkey1 as PK_ORDINAL1,
			//    sr.refkey2 as PK_ORDINAL2,
			//    sr.refkey3 as PK_ORDINAL3,
			//    sr.refkey4 as PK_ORDINAL4,
			//    sr.refkey5 as PK_ORDINAL5,
			//    sr.refkey6 as PK_ORDINAL6,
			//    sr.refkey7 as PK_ORDINAL7,
			//    sr.refkey8 as PK_ORDINAL8,
			//    sr.refkey9 as PK_ORDINAL9,
			//    sr.refkey10 as PK_ORDINAL10,
			//    sr.refkey11 as PK_ORDINAL11,
			//    sr.refkey12 as PK_ORDINAL12,
			//    sr.refkey13 as PK_ORDINAL13,
			//    sr.refkey14 as PK_ORDINAL14,
			//    sr.refkey15 as PK_ORDINAL15,
			//    sr.refkey16 as PK_ORDINAL16
			//from sysobjects sofk inner join sysreferences sr
			//    on sofk.id=sr.constrid
			//    inner join sysobjects sofkt
			//    on sr.tableid = sofkt.id
			//    inner join sysobjects sopkt
			//    on sr.reftabid = sopkt.id
			//    inner join sysusers sufk 
			//	  on sofkt.uid = sufk.uid
			//	  inner join sysusers supk
			//	  on sopkt.uid = supk.uid
			//WHERE sofk.type='RI'
			//     AND sufk.name IN ('<schema1>', .. '<schema n>')
			//     AND supk.name IN ('<schema1>', .. '<schema n>')
			#endregion

			string inClause = String.Join(", ", catalogMetaData.Schemas.Select(s=>string.Format("'{0}'", s.SchemaOwner)).ToArray());
			string query = string.Format("select sofk.name as FK_NAME, sufk.name as FK_SCHEMA_NAME, sofkt.name as FK_TABLE_NAME, supk.name as PK_SCHEMA_NAME, sopkt.name as PK_TABLE_NAME, sr.fokey1 as FK_ORDINAL1, sr.fokey2 as FK_ORDINAL2," +
				" sr.fokey3 as FK_ORDINAL3, sr.fokey4 as FK_ORDINAL4, sr.fokey5 as FK_ORDINAL5, sr.fokey6 as FK_ORDINAL6, sr.fokey7 as FK_ORDINAL7, sr.fokey8 as FK_ORDINAL8," +
				" sr.fokey9 as FK_ORDINAL9, sr.fokey10 as FK_ORDINAL10, sr.fokey11 as FK_ORDINAL11, sr.fokey12 as FK_ORDINAL12, sr.fokey13 as FK_ORDINAL13," +
				" sr.fokey14 as FK_ORDINAL14, sr.fokey15 as FK_ORDINAL15, sr.fokey16 as FK_ORDINAL16, sr.refkey1 as PK_ORDINAL1, sr.refkey2 as PK_ORDINAL2," +
				" sr.refkey3 as PK_ORDINAL3, sr.refkey4 as PK_ORDINAL4, sr.refkey5 as PK_ORDINAL5, sr.refkey6 as PK_ORDINAL6, sr.refkey7 as PK_ORDINAL7," +
				" sr.refkey8 as PK_ORDINAL8, sr.refkey9 as PK_ORDINAL9, sr.refkey10 as PK_ORDINAL10, sr.refkey11 as PK_ORDINAL11, sr.refkey12 as PK_ORDINAL12," +
				" sr.refkey13 as PK_ORDINAL13, sr.refkey14 as PK_ORDINAL14, sr.refkey15 as PK_ORDINAL15, sr.refkey16 as PK_ORDINAL16 from sysobjects sofk " +
				" inner join sysreferences sr on sofk.id=sr.constrid inner join sysobjects sofkt on sr.tableid = sofkt.id inner join sysobjects sopkt on sr.reftabid = sopkt.id inner join sysusers sufk on sofkt.uid = sufk.uid" +
				" inner join sysusers supk on sopkt.uid = supk.uid WHERE sofk.type='RI' AND sufk.name IN ({0}) AND supk.name IN ({0})", inClause);

			DbDataAdapter adapter = this.DriverToUse.CreateDataAdapter(query);
			DataTable foreignKeys = new DataTable();
			adapter.Fill(foreignKeys);

			foreach(DataRow fkRow in foreignKeys.AsEnumerable())
			{
				string fkName = fkRow.Value<string>("FK_NAME");
				DBForeignKeyConstraint newForeignKeyConstraint = new DBForeignKeyConstraint();
				DBSchema schemaForeignKey = catalogMetaData.FindSchemaByName(fkRow.Value<string>("FK_SCHEMA_NAME"));
				if(schemaForeignKey == null)
				{
					continue;
				}
				DBTable tableForeignKey = schemaForeignKey.FindTableByName(fkRow.Value<string>("FK_TABLE_NAME"));

				// Get Primary Key Table, first get the schema, has to be there
				DBSchema schemaPrimaryKey = catalogMetaData.FindSchemaByName(fkRow.Value<string>("PK_SCHEMA_NAME"));
				if(schemaPrimaryKey == null)
				{
					continue;
				}
				DBTable tablePrimaryKey = schemaPrimaryKey.FindTableByName(fkRow.Value<string>("PK_TABLE_NAME"));
				if((tableForeignKey == null) || (tablePrimaryKey == null))
				{
					continue;
				}

				// Add to Foreign Key table. 
				tableForeignKey.ForeignKeyConstraints.Add(newForeignKeyConstraint);
				newForeignKeyConstraint.AppliesToTable = tableForeignKey;
				newForeignKeyConstraint.ConstraintName = fkName;
				for(int j = 1; j <= 16; j++)
				{
					// sybase stores the pk/fk fields horizontally, 16 fields in 1 row (they're apparently not that confident about the concept of a 'table' ;) )
					int pkOrdinal = fkRow.Value<int>("PK_ORDINAL" + j);
					if(pkOrdinal <= 0)
					{
						// done
						break;
					}
					int fkOrdinal = fkRow.Value<int>("FK_ORDINAL" + j);
					DBTableField foreignKeyField = tableForeignKey.Fields[fkOrdinal-1];
					DBTableField primaryKeyField = tablePrimaryKey.Fields[pkOrdinal-1];
					newForeignKeyConstraint.PrimaryKeyFields.Add(primaryKeyField);
					newForeignKeyConstraint.ForeignKeyFields.Add(foreignKeyField);
				}
			}
		}
		

		/// <summary>
		/// Produces the additional actions to perform by this catalog retriever
		/// </summary>
		/// <returns>list of additional actions to perform per schema</returns>
		private List<CatalogMetaDataRetrievalActionDescription> ProduceAdditionalActionsToPerform()
		{
			List<CatalogMetaDataRetrievalActionDescription> toReturn = new List<CatalogMetaDataRetrievalActionDescription>();
			toReturn.Add(new CatalogMetaDataRetrievalActionDescription("Retrieving all Foreign Key Constraints", (catalog) => RetrieveForeignKeys(catalog), false));
			return toReturn;
		}


		#region Class Property Declarations
		/// <summary>
		/// Gets the additional actions to perform per schema.
		/// </summary>
		protected override List<CatalogMetaDataRetrievalActionDescription> AdditionalActionsPerSchema
		{
			get { return ProduceAdditionalActionsToPerform(); }
		}
		#endregion
	}
}
