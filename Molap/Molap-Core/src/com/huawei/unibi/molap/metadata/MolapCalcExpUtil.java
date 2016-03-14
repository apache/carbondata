/**
 * 
 */
package com.huawei.unibi.molap.metadata;


/**
 * @author R00900208
 *
 */
public class MolapCalcExpUtil
{
//    /**
//     * This method resolves the Calculated measure. 
//     * 
//     * @param xmlCalcMembers
//     * @param cube
//     * @return
//     *
//     */
//    public static Query resolveCalcMembers(
//            List<MondrianDef.CalculatedMember> xmlCalcMembers,
//            RolapCube cube)
//        {
//            // If there are no objects to create, our generated SQL will be so
//            // silly, the parser will laugh.
//            if (xmlCalcMembers.size() == 0) 
//            {
//                return null;
//            }
//
//            StringBuilder buf = new StringBuilder(256);
//            buf.append("WITH").append(Util.nl);
//
//            // Check the members individually, and generate SQL.
//            final Set<String> fqNames = new LinkedHashSet<String>();
//            for (int i = 0; i < xmlCalcMembers.size(); i++) {
//                preCalcMember(xmlCalcMembers, i, buf, cube, fqNames);
//            }
//
////            // Check the named sets individually (for uniqueness) and generate SQL.
////            Set<String> nameSet = new HashSet<String>();
////            for (Formula namedSet : namedSetList) {
////                nameSet.add(namedSet.getName());
////            }
////            for (MondrianDef.NamedSet xmlNamedSet : xmlNamedSets) {
////                preNamedSet(xmlNamedSet, nameSet, buf);
////            }
//
//            buf.append("SELECT FROM ").append(cube.getUniqueName());
//
//            // Parse and validate this huge MDX query we've created.
//            final String queryString = buf.toString();
//            final Query queryExp;
//            try {
//                RolapConnection conn = cube.getSchema().getInternalConnection();
//                queryExp = conn.parseQuery(queryString, false);
//            } catch (Exception e) {
//                throw MondrianResource.instance().UnknownNamedSetHasBadFormula.ex(
//                    cube.getName(), e);
//            }
//            queryExp.resolve();
//            return queryExp;
//        }
//    
//    private static void preCalcMember(
//            List<MondrianDef.CalculatedMember> xmlCalcMembers,
//            int j,
//            StringBuilder buf,
//            RolapCube cube,
//            Set<String> fqNames)
//        {
//            MondrianDef.CalculatedMember xmlCalcMember = xmlCalcMembers.get(j);
//
//            // Lookup dimension
//            Hierarchy hierarchy = null;
//            String dimName = null;
//            if (xmlCalcMember.dimension != null) {
//                dimName = xmlCalcMember.dimension;
//                final Dimension dimension =
//                    cube.lookupDimension(
//                        new Id.Segment(
//                            xmlCalcMember.dimension,
//                            Id.Quoting.UNQUOTED));
//                if (dimension != null) {
//                    hierarchy = dimension.getHierarchy();
//                }
//            } else if (xmlCalcMember.hierarchy != null) {
//                dimName = xmlCalcMember.hierarchy;
//                hierarchy = (Hierarchy)
//                    cube.getSchemaReader().lookupCompound(
//                        cube,
//                        Util.parseIdentifier(dimName),
//                        false,
//                        Category.Hierarchy);
//            }
//            if (hierarchy == null) {
//                throw MondrianResource.instance().CalcMemberHasBadDimension.ex(
//                    dimName, xmlCalcMember.name, cube.getName());
//            }
//
//            // Root of fully-qualified name.
//            String parentFqName;
//            if (xmlCalcMember.parent != null) {
//                parentFqName = xmlCalcMember.parent;
//            } else {
//                parentFqName = hierarchy.getUniqueNameSsas();
//            }
//
//            // If we're processing a virtual cube, it's possible that we've
//            // already processed this calculated member because it's
//            // referenced in another measure; in that case, remove it from the
//            // list, since we'll add it back in later; otherwise, in the
//            // non-virtual cube case, throw an exception
//            final String fqName = Util.makeFqName(parentFqName, xmlCalcMember.name);
////            for (int i = 0; i < calculatedMemberList.size(); i++) {
////                Formula formula = calculatedMemberList.get(i);
////                if (formula.getName().equals(xmlCalcMember.name)
////                    && formula.getMdxMember().getHierarchy().equals(
////                        hierarchy))
////                {
////                    if (errOnDup) {
////                        throw MondrianResource.instance().CalcMemberNotUnique.ex(
////                            fqName,
////                            getName());
////                    } else {
////                        calculatedMemberList.remove(i);
////                        --i;
////                    }
////                }
////            }
//
//            // Check this calc member doesn't clash with one earlier in this
//            // batch.
//            if (!fqNames.add(fqName)) {
//                throw MondrianResource.instance().CalcMemberNotUnique.ex(
//                    fqName,
//                    cube.getName());
//            }
//
//            final MondrianDef.CalculatedMemberProperty[] xmlProperties =
//                    xmlCalcMember.memberProperties;
//            List<String> propNames = new ArrayList<String>();
//            List<String> propExprs = new ArrayList<String>();
//            validateMemberProps(
//                xmlProperties, propNames, propExprs, xmlCalcMember.name,cube);
//
//            final int measureCount =14;
//
//            // Generate SQL.
//            assert fqName.startsWith("[");
//            buf.append("MEMBER ")
//                .append(fqName)
//                .append(Util.nl)
//                .append("  AS ");
//            Util.singleQuoteString(xmlCalcMember.getFormula(), buf);
//
//            if (xmlCalcMember.cellFormatter != null) {
//                if (xmlCalcMember.cellFormatter.className != null) {
//                    propNames.add(Property.CELL_FORMATTER.name);
//                    propExprs.add(
//                        Util.quoteForMdx(xmlCalcMember.cellFormatter.className));
//                }
//                if (xmlCalcMember.cellFormatter.script != null) {
//                    if (xmlCalcMember.cellFormatter.script.language != null) {
//                        propNames.add(Property.CELL_FORMATTER_SCRIPT_LANGUAGE.name);
//                        propExprs.add(
//                            Util.quoteForMdx(
//                                xmlCalcMember.cellFormatter.script.language));
//                    }
//                    propNames.add(Property.CELL_FORMATTER_SCRIPT.name);
//                    propExprs.add(
//                        Util.quoteForMdx(xmlCalcMember.cellFormatter.script.cdata));
//                }
//            }
//
//            assert propNames.size() == propExprs.size();
//            processFormatStringAttribute(xmlCalcMember, buf);
//
//            for (int i = 0; i < propNames.size(); i++) {
//                String name = propNames.get(i);
//                String expr = propExprs.get(i);
//                buf.append(",").append(Util.nl);
//                expr = removeSurroundingQuotesIfNumericProperty(name, expr);
//                buf.append(name).append(" = ").append(expr);
//            }
//            // Flag that the calc members are defined against a cube; will
//            // determine the value of Member.isCalculatedInQuery
//            buf.append(",")
//                .append(Util.nl);
//            Util.quoteMdxIdentifier(Property.MEMBER_SCOPE.name, buf);
//            buf.append(" = 'CUBE'");
//
//            // Assign the member an ordinal higher than all of the stored measures.
//            if (!propNames.contains(Property.MEMBER_ORDINAL.getName())) {
//                buf.append(",")
//                    .append(Util.nl)
//                    .append(Property.MEMBER_ORDINAL)
//                    .append(" = ")
//                    .append(measureCount + j);
//            }
//            buf.append(Util.nl);
//        }
//    
//    private static void processFormatStringAttribute(
//            MondrianDef.CalculatedMember xmlCalcMember,
//            StringBuilder buf)
//        {
//            if (xmlCalcMember.formatString != null) {
//                buf.append(",")
//                    .append(Util.nl)
//                    .append(Property.FORMAT_STRING.name)
//                    .append(" = ")
//                    .append(Util.quoteForMdx(xmlCalcMember.formatString));
//            }
//        }
//    
//    private static String removeSurroundingQuotesIfNumericProperty(
//            String name,
//            String expr)
//        {
//            Property prop = Property.lookup(name, false);
//            if (prop != null
//                && prop.getType() == Property.Datatype.TYPE_NUMERIC
//                && isSurroundedWithQuotes(expr)
//                && expr.length() > 2)
//            {
//                return expr.substring(1, expr.length() - 1);
//            }
//            return expr;
//        }
//    
//    private static boolean isSurroundedWithQuotes(String expr) {
//        return expr.startsWith("\"") && expr.endsWith("\"");
//    }
//    
//    /**
//     * Validates an array of member properties, and populates a list of names
//     * and expressions, one for each property.
//     *
//     * @param xmlProperties Array of property definitions.
//     * @param propNames Output array of property names.
//     * @param propExprs Output array of property expressions.
//     * @param memberName Name of member which the properties belong to.
//     */
//    private static void validateMemberProps(
//        final MondrianDef.CalculatedMemberProperty[] xmlProperties,
//        List<String> propNames,
//        List<String> propExprs,
//        String memberName,RolapCube cube)
//    {
//        if (xmlProperties == null) {
//            return;
//        }
//        for (MondrianDef.CalculatedMemberProperty xmlProperty : xmlProperties) {
//            if (xmlProperty.expression == null && xmlProperty.value == null) {
//                throw MondrianResource.instance()
//                    .NeitherExprNorValueForCalcMemberProperty.ex(
//                        xmlProperty.name, memberName, cube.getName());
//            }
//            if (xmlProperty.expression != null && xmlProperty.value != null) {
//                throw MondrianResource.instance().ExprAndValueForMemberProperty
//                    .ex(
//                        xmlProperty.name, memberName, cube.getName());
//            }
//            propNames.add(xmlProperty.name);
//            if (xmlProperty.expression != null) {
//                propExprs.add(xmlProperty.expression);
//            } else {
//                propExprs.add(Util.quoteForMdx(xmlProperty.value));
//            }
//        }
//    }
}
