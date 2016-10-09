/** In factory design pattern, objects are created without exposing implementation details and creation logic to
 * client users. When creating process logic, it can be integrated into factory methods.
 *
 * In addition, factory method or a set of factory methods can provide more clear and descriptive way of
 * constructing objects, especially sometimes constructor overloading can make object constructing ambiguous.
 *
 * See wiki for more details https://en.wikipedia.org/wiki/Factory_(object-oriented_programming)#Design_patterns
 * Created by Haitao (James) Li on 9/10/16.
 */
public class ShapeFactory {
    interface Shape {
        void draw();
    }

    private class Rectangle implements Shape {
        @Override
        public void draw() {
            System.out.println("Rectangle");
        }
    }

    private class Triangle implements Shape {
        @Override
        public void draw() {
            System.out.println("Triangle");
        }
    }

    private class Square implements Shape {
        @Override
        public void draw() {
            System.out.println("Square");
        }
    }

    public Shape getShape(String shape) {
        if (shape == null) {
            return null;
        }
        if (shape.equalsIgnoreCase("Rectangle")) {
            return new Rectangle();
        }
        else if (shape.equalsIgnoreCase("Triangle")) {
            return new Triangle();
        }
        else if (shape.equalsIgnoreCase("Square")) {
            return new Square();
        }
        else {
            return null;
        }
    }
}
