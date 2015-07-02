package org.utad;
import java.io.Serializable;

public final class OlympicMedalRecords implements Serializable {
    private final String name;
    private final int age;
    private final String country;
    private final int olympicGame;
    private final String sport;
    private final int goldMedals;
    private final int silverMedals;
    private final int bronzeMedals;



    public OlympicMedalRecords(String name, int age, String country, int olympicGame, String sport, int goldMedals,
                               int silverMedals, int bronzeMedals) {
        this.name = name;
        this.age = age;
        this.country = country;
        this.olympicGame = olympicGame;
        this.sport = sport;
        this.goldMedals = goldMedals;
        this.silverMedals = silverMedals;
        this.bronzeMedals = bronzeMedals;

    }

    public String getName() {
        return name;
    }

    public int getAge() {
        return age;
    }

    public String getCountry() {
        return country;
    }

    public int getOlympicGame() {
        return olympicGame;
    }

    public String getSport() {
        return sport;
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
