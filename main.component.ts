import { Component, OnInit } from '@angular/core';

@Component({
  selector: 'app-date-list',
  templateUrl: './date-list.component.html',
  styleUrls: ['./date-list.component.css']
})
export class DateListComponent implements OnInit {
  dates: Date[] = [];

  ngOnInit(): void {
    const today = new Date();
    for (let i = 0; i < 30; i++) {
      const nextDay = new Date(today);
      nextDay.setDate(today.getDate() + i);
      this.dates.push(nextDay);
    }
  }
}
