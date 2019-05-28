package model;

public class Student {
    public int id;
    public String name;
    public String password;
    public int age;

    public Student(){

    }
    public Student(int id, String name, String password, int age) {
        this.id=id;
        this.name=name;
        this.password=password;
        this.age=age;
    }
    @Override
    public String toString() {
        return "model.Student{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", password='" + password + '\'' +
                ", age=" + age +
                '}';
    }
    public int getId(){
        return id;
    }
    public void setId() {
        this.id=id;
    }
    public String getName() {
        return name;
    }
    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

}
