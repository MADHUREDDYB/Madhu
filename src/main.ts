import { provideZonelessChangeDetection } from '@angular/core';
import { bootstrapApplication } from '@angular/platform-browser';
import { DateListComponent } from '../main.component';

bootstrapApplication(DateListComponent, {
  providers: [provideZonelessChangeDetection()],
}).catch((error: unknown) => console.error(error));
