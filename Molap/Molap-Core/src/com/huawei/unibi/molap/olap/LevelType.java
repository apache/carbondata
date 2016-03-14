package com.huawei.unibi.molap.olap;



/**
 * Enumerates the types of levels.
 *
 * @deprecated Will be replaced with {@link org.olap4j.metadata.Level.Type}
 * before mondrian-4.0.
 *
 * @author jhyde
 * @since 5 April, 2004
 * @version $Id: //open/mondrian/src/main/mondrian/olap/LevelType.java#15 $
 */
public enum LevelType {

    /** Indicates that the level is not related to time. */
    Regular,

    /**
     * Indicates that a level refers to years.
     * It must be used in a dimension whose type is
     * {@link DimensionType#TimeDimension}.
     */
    TimeYears,

    /**
     * Indicates that a level refers to half years.
     * It must be used in a dimension whose type is
     * {@link DimensionType#TimeDimension}.
     */
    TimeHalfYears,

    /**
     * Indicates that a level refers to quarters.
     * It must be used in a dimension whose type is
     * {@link DimensionType#TimeDimension}.
     */
    TimeQuarters,

    /**
     * Indicates that a level refers to months.
     * It must be used in a dimension whose type is
     * {@link DimensionType#TimeDimension}.
     */
    TimeMonths,

    /**
     * Indicates that a level refers to weeks.
     * It must be used in a dimension whose type is
     * {@link DimensionType#TimeDimension}.
     */
    TimeWeeks,

    /**
     * Indicates that a level refers to days.
     * It must be used in a dimension whose type is
     * {@link DimensionType#TimeDimension}.
     */
    TimeDays,

    /**
     * Indicates that a level refers to hours.
     * It must be used in a dimension whose type is
     * {@link DimensionType#TimeDimension}.
     */
    TimeHours,

    /**
     * Indicates that a level refers to minutes.
     * It must be used in a dimension whose type is
     * {@link DimensionType#TimeDimension}.
     */
    TimeMinutes,

    /**
     * Indicates that a level refers to seconds.
     * It must be used in a dimension whose type is
     * {@link DimensionType#TimeDimension}.
     */
    TimeSeconds,

    /**
     * Indicates that a level is an unspecified time period.
     * It must be used in a dimension whose type is
     * {@link DimensionType#TimeDimension}.
     */
    TimeUndefined,

    /**
     * Indicates that a level holds the null member.
     */
    Null;

    /**
     * Returns whether this is a time level.
     *
     * @return Whether this is a time level.
     */
    public boolean isTime() {
        return ordinal() >= TimeYears.ordinal()
           && ordinal() <= TimeUndefined.ordinal();
    }
}

// End LevelType.java
