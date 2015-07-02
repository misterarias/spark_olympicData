package org.utad;

import java.io.Serializable;

public class OlympicAthletes implements Serializable {
    private final String name;
    private final int yearFirstMedal;
    private final int yearLastMedal;
    private final int goldMedals;
    private final int silverMedals;
    private final int bronzeMedals;

    public OlympicAthletes(String name, int yearFirstMedal, int yearLastMedal, int goldMedals, int silverMedals, int bronzeMedals) {
        this.name = name;
        this.yearFirstMedal = yearFirstMedal;
        this.yearLastMedal = yearLastMedal;
        this.goldMedals = goldMedals;
        this.silverMedals = silverMedals;
        this.bronzeMedals = bronzeMedals;
    }

    public String getName() {
        return name;
    }

    public int getYearFirstMedal() {
        return yearFirstMedal;
    }

    public int getYearLastMedal() {
        return yearLastMedal;
    }

    public int getGoldMedals() {
        return goldMedals;
    }

    public int getSilverMedals() {
        return silverMedals;
    }

    public int getBronzeMedals() {
        return bronzeMedals;
    }
}
