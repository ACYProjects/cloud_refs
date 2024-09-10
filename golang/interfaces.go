package main

import (
	"fmt"
	"math"
)

type Shape interface {
	Area() float64
	Perimeter() float64
}

type Circle struct {
	Radius float64
}

func (c Circle) Area() float64 {
	return math.Pi * c.Radius * c.Radius
}

func (c Circle) Perimeter() float64 {
	return 2 * math.Pi * c.Radius
}

type Rectangle struct {
	Width  float64
	Height float64
}

func (r Rectangle) Area() float64 {
	return r.Width * r.Height
}

func (r Rectangle) Perimeter() float64 {
	return 2*r.Width + 2*r.Height
}

type Triangle struct {
	A, B, C float64 // sides of the triangle
}

func (t Triangle) Area() float64 {
	s := (t.A + t.B + t.C) / 2
	return math.Sqrt(s * (s - t.A) * (s - t.B) * (s - t.C))
}

func (t Triangle) Perimeter() float64 {
	return t.A + t.B + t.C
}

func PrintShapeInfo(s Shape) {
	fmt.Printf("Area: %.2f\n", s.Area())
	fmt.Printf("Perimeter: %.2f\n", s.Perimeter())
}

func main() {
	circle := Circle{Radius: 5}
	rectangle := Rectangle{Width: 10, Height: 5}
	triangle := Triangle{A: 3, B: 4, C: 5}

	fmt.Println("Circle:")
	PrintShapeInfo(circle)

	fmt.Println("\nRectangle:")
	PrintShapeInfo(rectangle)

	fmt.Println("\nTriangle:")
	PrintShapeInfo(triangle)

	// Demonstrating interface slices
	shapes := []Shape{circle, rectangle, triangle}

	fmt.Println("\nAreas of all shapes:")
	for _, shape := range shapes {
		fmt.Printf("%.2f\n", shape.Area())
	}
}
