package org.example;

import java.io.Serializable;
import java.util.Objects;

public class Nut implements Serializable {

    private String machineType;

    private String description;

    public String getMachineType() {
        return machineType;
    }

    public void setMachineType(String machineType) {
        this.machineType = machineType;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Nut nut = (Nut) o;
        return machineType.equals(nut.machineType) &&
          Objects.equals(description, nut.description);
    }

    @Override
    public int hashCode() {
        return Objects.hash(machineType, description);
    }
}
