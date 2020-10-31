M = csvread('Output.csv');
location = 'northwest'
elapsed_title = 'Total Elapsed Time '
avg_time_title = 'Average Time per Job '
mem_copy_title = 'Total Memory Copy Cost '
varied_parameter_title = ' Number of Workers'
with_arrivals_title = ''

figure()
f1 = plot(M(1,:), M(2,:), M(1,:), M(3,:), M(1,:), M(4,:), M(1,:), M(5,:),'LineWidth',2,'MarkerSize',6);
legend({'SRPT', 'FIFO', 'Round Robin', 'Max Weight'}, 'Location', location);
title(strcat(elapsed_title, ' versus ', varied_parameter_title, with_arrivals_title), 'FontSize', 12)
xlabel(varied_parameter_title,'FontSize', 20);
ylabel(elapsed_title,'FontSize', 20);
grid on;
set(gca,'linewidth', 1.2,'FontSize',18);

figure()
f2 = plot(M(1,:), M(6,:), M(1,:), M(7,:), M(1,:), M(8,:), M(1,:), M(9,:),'LineWidth',2,'MarkerSize',6);
legend({'SRPT', 'FIFO', 'Round Robin', 'Max Weight'}, 'Location', location);
title(strcat(avg_time_title, ' versus ', varied_parameter_title, with_arrivals_title), 'FontSize', 12)
xlabel(varied_parameter_title,'FontSize', 20);
ylabel(avg_time_title,'FontSize', 20);
grid on;
set(gca,'linewidth', 1.2,'FontSize',18);

figure()
f3 = plot(M(1,:), M(10,:), M(1,:), M(11,:), M(1,:), M(12,:), M(1,:), M(13,:),'LineWidth',2,'MarkerSize',6);
legend({'SRPT', 'FIFO', 'Round Robin', 'Max Weight'}, 'Location', location);
title(strcat(mem_copy_title, ' versus ', varied_parameter_title, with_arrivals_title), 'FontSize', 12)
xlabel(varied_parameter_title,'FontSize', 20);
ylabel(mem_copy_title,'FontSize', 20);
grid on;
set(gca,'linewidth', 1.2,'FontSize',18);