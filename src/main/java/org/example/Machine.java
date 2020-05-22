package org.example;

import java.io.Serializable;
import java.util.Objects;

public class Machine implements Serializable {

    private String id;
    private String machineType;

    public String getMachineType() {
        return machineType;
    }

    public void setMachineType(String machineType) {
        this.machineType = machineType;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Machine machine = (Machine) o;
        return id.equals(machine.id) &&
          Objects.equals(machineType, machine.machineType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, machineType);
    }
}
