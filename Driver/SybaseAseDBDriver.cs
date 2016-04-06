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
using System.Text;
using System.Linq;
using SD.Tools.BCLExtensions.CollectionsRelated;
using SD.Tools.BCLExtensions.DataRelated;
using SD.LLBLGen.Pro.DBDriverCore;
using System.Collections.Generic;
using System.Data.Common;

namespace SD.LLBLGen.Pro.DBDrivers.SybaseAse
{
	/// <summary>
	/// General implementation of the SybaseAse DBDriver.
	/// </summary>
	public class SybaseAseDBDriver : DBDriverBase
	{
		#region Constants
		private const string driverType = "Sybase ASE DBDriver";
		private const string driverVersion = "5.0.20160127";
		private const string driverVendor = "Solutions Design bv";
		private const string driverCopyright = "(c)2002-2016 Solutions Design, all rights reserved.";
		private const string driverID = "A3076322-977C-4e28-BFF4-F25ED096D1DB";
		#endregion


		/// <summary>
		/// CTor
		/// </summary>
		public SybaseAseDBDriver() : base((int)AseDbTypes.AmountOfSqlDbTypes, driverType, driverVendor, driverVersion, driverCopyright, driverID, "initial catalog")
		{
			InitDataStructures();
		}


		/// <summary>
		/// Fills the RDBMS functionality aspects.
		/// </summary>
		protected override void FillRdbmsFunctionalityAspects()
		{
			// SybaseAse will add values automatically for identity fields.
			this.RdbmsFunctionalityAspects.Add(RdbmsFunctionalityAspect.AutoGenerateIdentityFields);
			this.RdbmsFunctionalityAspects.Add(RdbmsFunctionalityAspect.CentralUnitIsCatalog);
			this.RdbmsFunctionalityAspects.Add(RdbmsFunctionalityAspect.SupportsForeignKeyConstraints);
			this.RdbmsFunctionalityAspects.Add(RdbmsFunctionalityAspect.SupportsMultipleSchemasPerCentralUnit);
			this.RdbmsFunctionalityAspects.Add(RdbmsFunctionalityAspect.SupportsMultipleCentralUnitsPerProject);
			this.RdbmsFunctionalityAspects.Add(RdbmsFunctionalityAspect.SupportsSchemaOnlyResultsetRetrieval);
			this.RdbmsFunctionalityAspects.Add(RdbmsFunctionalityAspect.SupportsNaturalCharacterSpecificTypes);
		}


		/// <summary>
		/// Fills the db type arrays for various conversions
		/// </summary>
		protected override void  FillDbTypeConvertArrays()
		{
			this.DBTypesAsProviderType[(int)AseDbTypes.BigInt] = "BigInt";
			this.DBTypesAsProviderType[(int)AseDbTypes.Binary] = "Binary";
			this.DBTypesAsProviderType[(int)AseDbTypes.Bit] = "Bit";
			this.DBTypesAsProviderType[(int)AseDbTypes.Char] = "Char";
			this.DBTypesAsProviderType[(int)AseDbTypes.Date] = "Date";
			this.DBTypesAsProviderType[(int)AseDbTypes.DateTime] = "DateTime";
			this.DBTypesAsProviderType[(int)AseDbTypes.Decimal] = "Decimal";
			this.DBTypesAsProviderType[(int)AseDbTypes.Double] = "Double";
			this.DBTypesAsProviderType[(int)AseDbTypes.FloatReal] = "Real";
			this.DBTypesAsProviderType[(int)AseDbTypes.FloatDouble] = "Double";
			this.DBTypesAsProviderType[(int)AseDbTypes.Integer] = "Integer";
			this.DBTypesAsProviderType[(int)AseDbTypes.Image] = "Image";
			this.DBTypesAsProviderType[(int)AseDbTypes.Money] = "Money";
			this.DBTypesAsProviderType[(int)AseDbTypes.NChar] = "NChar";
			this.DBTypesAsProviderType[(int)AseDbTypes.Numeric] = "Numeric";
			this.DBTypesAsProviderType[(int)AseDbTypes.NVarChar] = "NVarChar";
			this.DBTypesAsProviderType[(int)AseDbTypes.Real] = "Real";
			this.DBTypesAsProviderType[(int)AseDbTypes.SmallDateTime] = "SmallDateTime";
			this.DBTypesAsProviderType[(int)AseDbTypes.SmallInt] = "SmallInt";
			this.DBTypesAsProviderType[(int)AseDbTypes.SmallMoney] = "SmallMoney";
			this.DBTypesAsProviderType[(int)AseDbTypes.Text] = "Text";
			this.DBTypesAsProviderType[(int)AseDbTypes.Time] = "Time";
			this.DBTypesAsProviderType[(int)AseDbTypes.TimeStamp] = "TimeStamp";
			this.DBTypesAsProviderType[(int)AseDbTypes.TinyInt] = "TinyInt";
			this.DBTypesAsProviderType[(int)AseDbTypes.UniChar] = "UniChar";
			this.DBTypesAsProviderType[(int)AseDbTypes.UniText] = "Unitext";
			this.DBTypesAsProviderType[(int)AseDbTypes.UniVarChar] = "UniVarChar";
			this.DBTypesAsProviderType[(int)AseDbTypes.UnsignedBigInt] = "UnsignedBigInt";
			this.DBTypesAsProviderType[(int)AseDbTypes.UnsignedInt] = "UnsignedInt";
			this.DBTypesAsProviderType[(int)AseDbTypes.UnsignedSmallInt] = "UnsignedSmallInt";
			this.DBTypesAsProviderType[(int)AseDbTypes.VarBinary] = "VarBinary";
			this.DBTypesAsProviderType[(int)AseDbTypes.VarChar] = "VarChar";

			this.DBTypesAsNETType[(int)AseDbTypes.BigInt] = typeof(Int64);
			this.DBTypesAsNETType[(int)AseDbTypes.Binary] = typeof(Byte[]);
			this.DBTypesAsNETType[(int)AseDbTypes.Bit] = typeof(Boolean);
			this.DBTypesAsNETType[(int)AseDbTypes.Char] = typeof(String);
			this.DBTypesAsNETType[(int)AseDbTypes.Date] = typeof(DateTime);
			this.DBTypesAsNETType[(int)AseDbTypes.DateTime] = typeof(DateTime);
			this.DBTypesAsNETType[(int)AseDbTypes.Decimal] = typeof(Decimal);
			this.DBTypesAsNETType[(int)AseDbTypes.Double] = typeof(Double);
			this.DBTypesAsNETType[(int)AseDbTypes.FloatReal] = typeof(Single);
			this.DBTypesAsNETType[(int)AseDbTypes.FloatDouble] = typeof(Double);
			this.DBTypesAsNETType[(int)AseDbTypes.Integer] = typeof(Int32);
			this.DBTypesAsNETType[(int)AseDbTypes.Image] = typeof(Byte[]);
			this.DBTypesAsNETType[(int)AseDbTypes.Money] = typeof(Decimal);
			this.DBTypesAsNETType[(int)AseDbTypes.NChar] = typeof(String);
			this.DBTypesAsNETType[(int)AseDbTypes.Numeric] = typeof(Decimal);
			this.DBTypesAsNETType[(int)AseDbTypes.NVarChar] = typeof(String);
			this.DBTypesAsNETType[(int)AseDbTypes.Real] = typeof(Single);
			this.DBTypesAsNETType[(int)AseDbTypes.SmallDateTime] = typeof(DateTime);
			this.DBTypesAsNETType[(int)AseDbTypes.SmallInt] = typeof(Int16);
			this.DBTypesAsNETType[(int)AseDbTypes.SmallMoney] = typeof(Decimal);
			this.DBTypesAsNETType[(int)AseDbTypes.Text] = typeof(String);
			this.DBTypesAsNETType[(int)AseDbTypes.Time] = typeof(DateTime);
			this.DBTypesAsNETType[(int)AseDbTypes.TimeStamp] = typeof(Byte[]);
			this.DBTypesAsNETType[(int)AseDbTypes.TinyInt] = typeof(Byte);
			this.DBTypesAsNETType[(int)AseDbTypes.UniChar] = typeof(string);
			this.DBTypesAsNETType[(int)AseDbTypes.UniText] = typeof(string);
			this.DBTypesAsNETType[(int)AseDbTypes.UniVarChar] = typeof(string);
			this.DBTypesAsNETType[(int)AseDbTypes.UnsignedBigInt] = typeof(UInt64);
			this.DBTypesAsNETType[(int)AseDbTypes.UnsignedInt] = typeof(UInt32);
			this.DBTypesAsNETType[(int)AseDbTypes.UnsignedSmallInt] = typeof(UInt16);
			this.DBTypesAsNETType[(int)AseDbTypes.VarBinary] = typeof(Byte[]);
			this.DBTypesAsNETType[(int)AseDbTypes.VarChar] = typeof(String);

			this.DBTypesAsString[(int)AseDbTypes.BigInt] = "bigint";
			this.DBTypesAsString[(int)AseDbTypes.Binary] = "binary";
			this.DBTypesAsString[(int)AseDbTypes.Bit] = "bit";
			this.DBTypesAsString[(int)AseDbTypes.Char] = "char";
			this.DBTypesAsString[(int)AseDbTypes.Date] = "date";
			this.DBTypesAsString[(int)AseDbTypes.DateTime] = "datetime";
			this.DBTypesAsString[(int)AseDbTypes.Decimal] = "decimal";
			this.DBTypesAsString[(int)AseDbTypes.Double] = "double";
			this.DBTypesAsString[(int)AseDbTypes.FloatReal] = "float";
			this.DBTypesAsString[(int)AseDbTypes.FloatDouble] = "float";
			this.DBTypesAsString[(int)AseDbTypes.Image] = "image";
			this.DBTypesAsString[(int)AseDbTypes.Integer] = "int";
			this.DBTypesAsString[(int)AseDbTypes.Money] = "money";
			this.DBTypesAsString[(int)AseDbTypes.NChar] = "nchar";
			this.DBTypesAsString[(int)AseDbTypes.Numeric] = "numeric";
			this.DBTypesAsString[(int)AseDbTypes.NVarChar] = "nvarchar";
			this.DBTypesAsString[(int)AseDbTypes.Real] = "real";
			this.DBTypesAsString[(int)AseDbTypes.SmallDateTime] = "smalldatetime";
			this.DBTypesAsString[(int)AseDbTypes.SmallInt] = "smallint";
			this.DBTypesAsString[(int)AseDbTypes.SmallMoney] = "smallmoney";
			this.DBTypesAsString[(int)AseDbTypes.Text] = "text";
			this.DBTypesAsString[(int)AseDbTypes.Time] = "timestamp";
			this.DBTypesAsString[(int)AseDbTypes.TimeStamp] = "timestamp";
			this.DBTypesAsString[(int)AseDbTypes.TinyInt] = "tinyint";
			this.DBTypesAsString[(int)AseDbTypes.UniChar] = "unichar";
			this.DBTypesAsString[(int)AseDbTypes.UniText] = "unitext";
			this.DBTypesAsString[(int)AseDbTypes.UniVarChar] = "univarchar";
			this.DBTypesAsString[(int)AseDbTypes.UnsignedBigInt] = "unsignedbigint";
			this.DBTypesAsString[(int)AseDbTypes.UnsignedInt] = "unsignedint";
			this.DBTypesAsString[(int)AseDbTypes.UnsignedSmallInt] = "unsignedsmallint";
			this.DBTypesAsString[(int)AseDbTypes.VarBinary] = "varbinary";
			this.DBTypesAsString[(int)AseDbTypes.VarChar] = "varchar";
		}


		/// <summary>
		/// Gets the string value of the db type passed in, from the Enum specification used in this driver for type specification
		/// </summary>
		/// <param name="dbType">The db type value.</param>
		/// <returns>string representation of the dbType specified when seen as a value in the type enum used by this driver to specify types.</returns>
		public override string GetDbTypeAsEnumStringValue(int dbType)
		{
			if(!Enum.IsDefined(typeof(AseDbTypes), dbType))
			{
				return "INVALID";
			}
			return ((AseDbTypes)dbType).ToString();
		}


		/// <summary>
		/// Returns the DBType value related to the type passed in in stringformat. The string is read
		/// from SybaseAse. 
		/// </summary>
		/// <param name="SybaseAseType">SybaseAse type in stringformat. Casing is not important.</param>
		/// <param name="lengthInBytes">The length in bytes. Used for floats</param>
		/// <returns>
		/// DBType value representing the same type as SybaseAseType
		/// </returns>
		internal static int ConvertStringToDBType(string SybaseAseType, int lengthInBytes)
		{
			int toReturn;

			switch(SybaseAseType.ToLowerInvariant())
			{
				case "bigint":
					toReturn = (int)AseDbTypes.BigInt;
					break;
				case "binary":
					toReturn = (int)AseDbTypes.Binary;
					break;
				case "bit":
					toReturn = (int)AseDbTypes.Bit;
					break;
				case "char":
					toReturn = (int)AseDbTypes.Char;
					break;
				case "date":
					toReturn = (int)AseDbTypes.Date;
					break;
				case "datetime":
					toReturn = (int)AseDbTypes.DateTime;
					break;
				case "decimal":
					toReturn = (int)AseDbTypes.Decimal;
					break;
				case "double":
					toReturn = (int)AseDbTypes.Double;
					break;
				case "float":
					if(lengthInBytes <= 4)
					{
						toReturn = (int)AseDbTypes.FloatReal;
					}
					else
					{
						toReturn = (int)AseDbTypes.FloatDouble;
					}
					break;
				case "image":
					toReturn = (int)AseDbTypes.Image;
					break;
				case "int":
				case "integer":
					toReturn = (int)AseDbTypes.Integer;
					break;
				case "money":
					toReturn = (int)AseDbTypes.Money;
					break;
				case "nchar":
					toReturn = (int)AseDbTypes.NChar;
					break;
				case "numeric":
					toReturn = (int)AseDbTypes.Numeric;
					break;
				case "nvarchar":
					toReturn = (int)AseDbTypes.NVarChar;
					break;
				case "real":
					toReturn = (int)AseDbTypes.Real;
					break;
				case "smalldatetime":
					toReturn = (int)AseDbTypes.SmallDateTime;
					break;
				case "smallint":
					toReturn = (int)AseDbTypes.SmallInt;
					break;
				case "smallmoney":
					toReturn = (int)AseDbTypes.SmallMoney;
					break;
				case "text":
					toReturn = (int)AseDbTypes.Text;
					break;
				case "time":
					toReturn = (int)AseDbTypes.Time;
					break;
				case "timestamp":
					toReturn = (int)AseDbTypes.TimeStamp;
					break;
				case "tinyint":
					toReturn = (int)AseDbTypes.TinyInt;
					break;
				case "unichar":
					toReturn = (int)AseDbTypes.UniChar;
					break;
				case "univarchar":
					toReturn = (int)AseDbTypes.UniVarChar;
					break;
				case "unitext":
					toReturn = (int)AseDbTypes.UniText;
					break;
				case "unsignedbigint":
					toReturn = (int)AseDbTypes.UnsignedBigInt;
					break;
				case "unsignedint":
					toReturn = (int)AseDbTypes.UnsignedInt;
					break;
				case "unsignedsmallint":
					toReturn = (int)AseDbTypes.UnsignedSmallInt;
					break;
				case "varbinary":
					toReturn = (int)AseDbTypes.VarBinary;
					break;
				case "varchar":
					toReturn = (int)AseDbTypes.VarChar;
					break;
				default:
					toReturn = (int)AseDbTypes.VarChar;
					break;
			}

			return toReturn;
		}


		/// <summary>
		/// Fills the NET to DB type conversions list.
		/// </summary>
		protected override void FillNETToDBTypeConversionsList()
		{
			this.NETToDBTypeConversions.Add(new NETToDBTypeConversion(typeof(byte[]), l => (l == 0) || (l >= 8192), null, null, (int)AseDbTypes.Image, 2147483647, 0, 0));
			this.NETToDBTypeConversions.Add(new NETToDBTypeConversion(typeof(byte[]), l => l == 8, null, null, (int)AseDbTypes.TimeStamp, -1, 0, 0));
			this.NETToDBTypeConversions.Add(new NETToDBTypeConversion(typeof(byte[]), l => (l > 0) && (l < 8192), null, null, (int)AseDbTypes.Binary, -1, 0, 0));
			this.NETToDBTypeConversions.Add(new NETToDBTypeConversion(typeof(byte[]), l => (l > 0) && (l < 8192), null, null, (int)AseDbTypes.VarBinary, -1, 0, 0));

			this.NETToDBTypeConversions.Add(new NETToDBTypeConversion(typeof(string), l => (l == 0) || (l >= 8192), null, null, (int)AseDbTypes.Text, 2147483647, 0, 0));
			this.NETToDBTypeConversions.Add(new NETToDBTypeConversion(typeof(string), l => (l == 0) || (l >= 8192), null, null, (int)AseDbTypes.UniText, 2147483647, 0, 0));
			this.NETToDBTypeConversions.Add(new NETToDBTypeConversion(typeof(string), l => (l > 0) && (l < 8192), null, null, (int)AseDbTypes.Char, -1, 0, 0));
			this.NETToDBTypeConversions.Add(new NETToDBTypeConversion(typeof(string), l => (l > 0) && (l < 8192), null, null, (int)AseDbTypes.NChar, -1, 0, 0));
			this.NETToDBTypeConversions.Add(new NETToDBTypeConversion(typeof(string), l => (l > 0) && (l < 8192), null, null, (int)AseDbTypes.NVarChar, -1, 0, 0));
			this.NETToDBTypeConversions.Add(new NETToDBTypeConversion(typeof(string), l => (l > 0) && (l < 8192), null, null, (int)AseDbTypes.UniChar, -1, 0, 0));
			this.NETToDBTypeConversions.Add(new NETToDBTypeConversion(typeof(string), l => (l > 0) && (l < 8192), null, null, (int)AseDbTypes.UniVarChar, -1, 0, 0));
			this.NETToDBTypeConversions.Add(new NETToDBTypeConversion(typeof(string), l => (l > 0) && (l < 8192), null, null, (int)AseDbTypes.VarChar, -1, 0, 0));

			this.NETToDBTypeConversions.Add(new NETToDBTypeConversion(typeof(DateTime), (int)AseDbTypes.Date, 0, 0, 0));
			this.NETToDBTypeConversions.Add(new NETToDBTypeConversion(typeof(DateTime), (int)AseDbTypes.DateTime, 0, 0, 0));
			this.NETToDBTypeConversions.Add(new NETToDBTypeConversion(typeof(DateTime), (int)AseDbTypes.SmallDateTime, 0, 0, 0));
			this.NETToDBTypeConversions.Add(new NETToDBTypeConversion(typeof(DateTime), (int)AseDbTypes.Time, 0, 0, 0));

			this.NETToDBTypeConversions.Add(new NETToDBTypeConversion(typeof(decimal), null, null, s => s == 0, (int)AseDbTypes.Numeric, 0, -1, -1));
			this.NETToDBTypeConversions.Add(new NETToDBTypeConversion(typeof(decimal), null, p => p <= 9, s => s > 0 && s <= 4, (int)AseDbTypes.SmallMoney, 0, -1, -1));
			this.NETToDBTypeConversions.Add(new NETToDBTypeConversion(typeof(decimal), null, null, s => s > 0 && s <= 4, (int)AseDbTypes.Money, 0, -1, -1));
			this.NETToDBTypeConversions.Add(new NETToDBTypeConversion(typeof(decimal), (int)AseDbTypes.Decimal, 0, -1, -1));

			this.NETToDBTypeConversions.Add(new NETToDBTypeConversion(typeof(float), (int)AseDbTypes.Real, 0, -1, 0));
			this.NETToDBTypeConversions.Add(new NETToDBTypeConversion(typeof(double), (int)AseDbTypes.Double, 0, -1, 0));
			this.NETToDBTypeConversions.Add(new NETToDBTypeConversion(typeof(long), (int)AseDbTypes.BigInt, 0, 19, 0));
			this.NETToDBTypeConversions.Add(new NETToDBTypeConversion(typeof(bool), (int)AseDbTypes.Bit, 0, 0, 0));
			this.NETToDBTypeConversions.Add(new NETToDBTypeConversion(typeof(int), (int)AseDbTypes.Integer, 0, 10, 0));
			this.NETToDBTypeConversions.Add(new NETToDBTypeConversion(typeof(short), (int)AseDbTypes.SmallInt, 0, 5, 0));
			this.NETToDBTypeConversions.Add(new NETToDBTypeConversion(typeof(byte), (int)AseDbTypes.TinyInt, 0, 3, 0));
			this.NETToDBTypeConversions.Add(new NETToDBTypeConversion(typeof(UInt64), (int)AseDbTypes.UnsignedBigInt, 0, 19, 0));
			this.NETToDBTypeConversions.Add(new NETToDBTypeConversion(typeof(UInt32), (int)AseDbTypes.UnsignedInt, 0, 10, 0));
			this.NETToDBTypeConversions.Add(new NETToDBTypeConversion(typeof(UInt16), (int)AseDbTypes.UnsignedSmallInt, 0, 5, 0));
		}


		/// <summary>
		/// Fills the DB type sort order list.
		/// </summary>
		protected override void FillDBTypeSortOrderList()
		{
			// byte[]
			this.SortOrderPerDBType.Add((int)AseDbTypes.Image, 0);
			this.SortOrderPerDBType.Add((int)AseDbTypes.VarBinary, 1);
			this.SortOrderPerDBType.Add((int)AseDbTypes.Binary, 2);

			// string
			this.SortOrderPerDBType.Add((int)AseDbTypes.Text, 0);
			this.SortOrderPerDBType.Add((int)AseDbTypes.UniText, 1);
			this.SortOrderPerDBType.Add((int)AseDbTypes.NChar, 2);
			this.SortOrderPerDBType.Add((int)AseDbTypes.Char, 3);
			this.SortOrderPerDBType.Add((int)AseDbTypes.UniChar, 4);
			this.SortOrderPerDBType.Add((int)AseDbTypes.NVarChar, 5);
			this.SortOrderPerDBType.Add((int)AseDbTypes.VarChar, 6);
			this.SortOrderPerDBType.Add((int)AseDbTypes.UniVarChar, 7);

			// datetime
			this.SortOrderPerDBType.Add((int)AseDbTypes.DateTime, 0);
			this.SortOrderPerDBType.Add((int)AseDbTypes.Date, 1);
			this.SortOrderPerDBType.Add((int)AseDbTypes.SmallDateTime, 2);
			this.SortOrderPerDBType.Add((int)AseDbTypes.Time, 3);

			// decimal
			this.SortOrderPerDBType.Add((int)AseDbTypes.Decimal, 0);
			this.SortOrderPerDBType.Add((int)AseDbTypes.Numeric, 1);
			this.SortOrderPerDBType.Add((int)AseDbTypes.SmallMoney, 2);
			this.SortOrderPerDBType.Add((int)AseDbTypes.Money, 3);
		}


		/// <summary>
		/// Gets the decimal types for this driver. Used for optimizing the SortOrder per db type.
		/// </summary>
		/// <returns>
		/// List with the db type values requested or an empty list if not applicable
		/// </returns>
		protected override List<int> GetDecimalTypes()
		{
			return new List<int> { (int)AseDbTypes.Decimal, (int)AseDbTypes.Numeric };
		}

		/// <summary>
		/// Gets the currency types for this driver. Used for optimizing the SortOrder per db type.
		/// </summary>
		/// <returns>
		/// List with the db type values requested or an empty list if not applicable
		/// </returns>
		protected override List<int> GetCurrencyTypes()
		{
			return new List<int> { (int)AseDbTypes.SmallMoney, (int)AseDbTypes.Money };
		}

		/// <summary>
		/// Gets the fixed length types (with multiple bytes, like char, binary), not b/clobs, for this driver. Used for optimizing the SortOrder per db type.
		/// </summary>
		/// <returns>
		/// List with the db type values requested or an empty list if not applicable
		/// </returns>
		/// <remarks>It's essential that natural character types are stored at a lower index than normal character types.</remarks>
		protected override List<int> GetFixedLengthTypes()
		{
			return new List<int> { (int)AseDbTypes.Binary, (int)AseDbTypes.NChar, (int)AseDbTypes.Char, (int)AseDbTypes.UniChar };
		}

		/// <summary>
		/// Gets the variable length types (with multiple bytes, like varchar, varbinary), not b/clobs, for this driver. Used for optimizing the SortOrder per
		/// db type.
		/// </summary>
		/// <returns>
		/// List with the db type values requested or an empty list if not applicable
		/// </returns>
		/// <remarks>It's essential that natural character types are stored at a lower index than normal character types.</remarks>
		protected override List<int> GetVariableLengthTypes()
		{
			return new List<int> { (int)AseDbTypes.VarBinary, (int)AseDbTypes.NVarChar, (int)AseDbTypes.VarChar, (int)AseDbTypes.UniVarChar };
		}


		/// <summary>
		/// Returns true if the specified type requires a value specified in Insert queries. Not all RDBMS's require for each type a value. F.e. SybaseAse
		/// doesn't require a value for Timestamp types, SybaseAse will insert a value automatically.
		/// </summary>
		/// <param name="dbType">Numeric DBType representation to check</param>
		/// <returns>true if the DBType is handled by the RDBMS automatically.</returns>
		public override bool DBTypeRequiresInsertValue(int dbType)
		{
			bool toReturn;

			switch(dbType)
			{
				// specify the types which do not require a value.
				case (int)AseDbTypes.TimeStamp:
					toReturn=false;
					break;
				default:
					toReturn=true;
					break;
			}

			return toReturn;
		}


		/// <summary>
		/// Returns true if the passed in type is a numeric type. 
		/// </summary>
		/// <param name="dbType">type to check</param>
		/// <returns>true if the type is a numeric type, false otherwise</returns>
		public override bool DBTypeIsNumeric(int dbType)
		{
			bool toReturn = false;

			switch((AseDbTypes)dbType)
			{
				case AseDbTypes.BigInt:
				case AseDbTypes.Decimal:
				case AseDbTypes.Double:
				case AseDbTypes.FloatDouble:
				case AseDbTypes.FloatReal:
				case AseDbTypes.Integer:
				case AseDbTypes.Money:
				case AseDbTypes.Numeric:
				case AseDbTypes.Real:
				case AseDbTypes.SmallInt:
				case AseDbTypes.SmallMoney:
				case AseDbTypes.TinyInt:
				case AseDbTypes.UnsignedBigInt:
				case AseDbTypes.UnsignedInt:
				case AseDbTypes.UnsignedSmallInt:
					toReturn = true;
					break;
			}

			return toReturn;
		}

		/// <summary>
		/// Produces the DBCatalogRetriever instance to use for retrieving meta-data of a catalog.
		/// </summary>
		/// <returns>ready to use catalog retriever object</returns>
		public override DBCatalogRetriever CreateCatalogRetriever()
		{
			return new SybaseAseCatalogRetriever(this);
		}


		/// <summary>
		/// Creates the connectiondata object to be used to obtain the required information for connecting to the database.
		/// </summary>
		/// <returns></returns>
		public override ConnectionDataBase CreateConnectionDataCollector()
		{
			return new SybaseAseConnectionData(this);
		}


		/// <summary>
		/// Gets all catalog names from the database system connected through the specified connection elements set. 
		/// </summary>
		/// <returns>List of all catalog names found in the connected system. By default it returns a list with 'Default' for systems which don't
		/// use catalogs.</returns>
		public override List<string> GetAllCatalogNames()
		{
			DbConnection connection = this.CreateConnection();

			try
			{
				connection.Open();
				DataTable catalogs = connection.GetSchema("Databases");

				// We'll filter out 5 catalogs: tempdb, model, master, sybsystemdb and sybsystemprocs
				List<string> toReturn = (from catalogRow in catalogs.AsEnumerable()
										 let catalogName = catalogRow.Value<string>("database_name")
										 where !new[] { "master", "tempdb", "model", "sybsystemdb", "sybsystemprocs" }.Contains(catalogName)
										 select catalogName).ToList();
				return toReturn;
			}
			finally
			{
				connection.SafeClose(true);
			}
		}


		/// <summary>
		/// Gets all schema names from the catalog with the name specified in the database system connected through the specified connection elements set.
		/// </summary>
		/// <param name="catalogName">Name of the catalog.</param>
		/// <returns>
		/// List of all schema names in the catalog specified. By default it returns a list with 'Default' for systems which don't use schemas.
		/// </returns>
		public override List<string> GetAllSchemaNames(string catalogName)
		{
			using(DbConnection connection = this.CreateConnection())
			{
				SetCatalogNameAsDefault(connection, catalogName);
				DbDataAdapter adapter = this.CreateDataAdapter(connection, "select name as SchemaName from sysusers where suid > 0");
				DataTable schemaNames = new DataTable();
				adapter.Fill(schemaNames);
				return (from row in schemaNames.AsEnumerable()
						select row.Value<string>("SchemaName")).ToList();
			}
		}


		/// <summary>
		/// Gets all table names in the schema in the catalog specified in the database system connected through the specified connection elements set.
		/// </summary>
		/// <param name="catalogName">Name of the catalog.</param>
		/// <param name="schemaName">Name of the schema.</param>
		/// <returns>
		/// List of all the table names (not synonyms) in the schema in the catalog specified. By default it returns an empty list.
		/// </returns>
		public override List<DBElementName> GetAllTableNames(string catalogName, string schemaName)
		{
			return GetElementNames(catalogName, schemaName, "'TABLE'", new string[] {});
		}


		/// <summary>
		/// Gets all view names in the schema in the catalog specified in the database system connected through the specified connection elements set.
		/// </summary>
		/// <param name="catalogName">Name of the catalog.</param>
		/// <param name="schemaName">Name of the schema.</param>
		/// <returns>
		/// List of all the view names (synonyms) in the schema in the catalog specified. By default it returns an empty list.
		/// </returns>
		public override List<DBElementName> GetAllViewNames(string catalogName, string schemaName)
		{
			return GetElementNames(catalogName, schemaName, "'VIEW'", new[] { "sysquerymetrics"});
		}


		/// <summary>
		/// Gets all stored procedure names in the schema in the catalog specified in the database system connected through the specified connection elements set.
		/// </summary>
		/// <param name="catalogName">Name of the catalog.</param>
		/// <param name="schemaName">Name of the schema.</param>
		/// <returns>
		/// List of all the stored procedure names in the schema in the catalog specified. By default it returns an empty list.
		/// </returns>
		public override List<DBElementName> GetAllStoredProcedureNames(string catalogName, string schemaName)
		{
			DbConnection connection = this.CreateConnection();
			DbCommand command = this.CreateStoredProcCallCommand(connection, "sp_stored_procedures");
			this.CreateParameter(command, "@sp_owner", schemaName);
			DbDataAdapter adapter = this.CreateDataAdapter(command);
			DataTable elementNames = new DataTable();

			try
			{
				SetCatalogNameAsDefault(connection, catalogName);
				adapter.Fill(elementNames);
				return (from row in elementNames.AsEnumerable()
						select new DBElementName(row.Value<string>("procedure_name"), row.Value<string>("remarks") ?? string.Empty)).ToList();
			}
			finally
			{
				connection.SafeClose(true);
			}
		}


		/// <summary>
		/// Gets all system sequence instances for the database targeted. System sequences are sequences which are system wide, like @@IDENTITY.
		/// </summary>
		/// <returns>
		/// List of system sequences for this database. By default it returns an empty list
		/// </returns>
		/// <remarks>Method is expected to produce these sequences without a database connection.</remarks>
		public override List<DBSequence> GetAllSystemSequences()
		{
			return new List<DBSequence> { new DBSequence("@@IDENTITY") };
		}


		/// <summary>
		/// Gets the target description of the target the driver is connected to, for display in a UI
		/// </summary>
		/// <returns>
		/// string usable to display in a UI which contains a description of the target the driver is connected to.
		/// </returns>
		public override string GetTargetDescription()
		{
			return string.Format("{0} (Server: {1}. Version: {2}.)", this.DBDriverType, this.ConnectionElements[ConnectionElement.ServerName],
				this.ServerVersion);
		}


		/// <summary>
		/// Constructs a valid connection string from the elements specified in the hashtable connectionElements.
		/// </summary>
		/// <param name="connectionElementsToUse">The connection elements to use when producing the connection string</param>
		/// <returns>
		/// A valid connection string which is usable to connect to the database to work with.
		/// </returns>
		public override string ConstructConnectionString(Dictionary<ConnectionElement, string> connectionElementsToUse)
		{
			StringBuilder connectionString = new StringBuilder();

			connectionString.AppendFormat("data source={0};initial catalog={1};User ID={2};Password={3};Port={4}",
				connectionElementsToUse.GetValue(ConnectionElement.ServerName) ?? string.Empty,
				connectionElementsToUse.GetValue(ConnectionElement.CatalogName) ?? string.Empty,
				(connectionElementsToUse.GetValue(ConnectionElement.UserID) ?? string.Empty).Replace(";", "';'"),
				(connectionElementsToUse.GetValue(ConnectionElement.Password) ?? string.Empty).Replace(";", "';'"),
				connectionElementsToUse.GetValue(ConnectionElement.PortNumber) ?? string.Empty);
			return connectionString.ToString();
		}


		/// <summary>
		/// Gets the DbProviderFactory invariant names to use for the factory. The first one which is found is used.
		/// </summary>
		/// <returns>list of invariant names</returns>
		protected override List<string> GetDbProviderFactoryInvariantNames()
		{
			return new List<string> { "Sybase.Data.AseClient" };
		}


		/// <summary>
		/// Gets the element names.
		/// </summary>
		/// <param name="catalogName">Name of the catalog.</param>
		/// <param name="schemaName">Name of the schema.</param>
		/// <param name="elementType">Type of the element.</param>
		/// <param name="namesToFilterOut">The names to filter out.</param>
		/// <returns></returns>
		private List<DBElementName> GetElementNames(string catalogName, string schemaName, string elementType, IEnumerable<string> namesToFilterOut)
		{
			DbConnection connection = this.CreateConnection();
			DbCommand command = this.CreateCommand(connection, "sp_tables");
			command.CommandType = CommandType.StoredProcedure;
			this.CreateParameter(command, "@table_owner", schemaName);
			this.CreateParameter(command, "@table_type", elementType);
			DbDataAdapter adapter = this.CreateDataAdapter(command);
			DataTable elementNames = new DataTable();

			try
			{
				SetCatalogNameAsDefault(connection, catalogName);
				adapter.Fill(elementNames);
				return (from row in elementNames.AsEnumerable()
						let elementName = row.Value<string>("table_name")
						where !namesToFilterOut.Contains(elementName)
						select new DBElementName(elementName, row.Value<string>("remarks") ?? string.Empty)).ToList();
			}
			finally
			{
				connection.SafeClose(true);
			}
		}
	}
}